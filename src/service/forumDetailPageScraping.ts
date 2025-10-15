import puppeteer, { Browser, Page, LaunchOptions } from "puppeteer";
import { ForumThread } from "../model/ForumThread";
import { ForumPost } from "../model/ForumPost";
import { ForumMedia } from "../model/ForumMedia";
import { sequelize } from "../config/database";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import dotenv from "dotenv";
import { S3Service } from "./s3Service";
import {
  createTempUserDataDir,
  deleteTempUserDataDir,
  clearSystemCaches,
  getMemoryUsage,
  clearBrowserCache,
  createBrowserConfig,
  delay,
} from "../utils";
import { PutObjectCommand } from "@aws-sdk/client-s3";
import { Op } from "sequelize";
dotenv.config();

export interface PostData {
  postId: number;
  author: string;
  content: string;
  postCreatedDate: string;
  likes: string;
  medias: [string, string][]; // [fullImageUrl, thumbImageUrl] pairs
}

interface MediaTask {
  url: string;
  postId: number;
  threadId: number;
  key: string;
  extension?: string; // Optional file extension for attachment pages
  existThumb: number; // 0 for full image, 1 for thumbnail
}

interface LoginCredentials {
  username: string;
  password: string;
}

class ForumDetailPageScraper {
  private browser: Browser | null = null;
  private page: Page | null = null;
  private readonly SITE_URL = "https://www.lpsg.com";
  private readonly FORUM_URL = `${this.SITE_URL}/forums/models-and-celebrities.17/`;
  private cookiesPath: string;
  private credentials: LoginCredentials;
  private mode: string;
  private s3Service: S3Service;
  private pagesScraped: number = 0;
  private readonly PAGES_BEFORE_RESTART: number = 20; // Changed from 100 to 20
  private tempDir: string = "";

  // Node distribution configuration
  private readonly NODE_INDEX = parseInt(process.env.NODE_INDEX || "0");
  private readonly NODE_COUNT = parseInt(process.env.NODE_COUNT || "1");

  // Memory monitoring configuration
  private readonly MIN_AVAILABLE_MEMORY_MB = 200; // 200MB minimum available memory
  private readonly MEMORY_CHECK_INTERVAL = 30000; // Check every 10 seconds
  private memoryCheckInterval: NodeJS.Timeout | null = null;

  constructor() {
    this.cookiesPath = path.join(__dirname, "../../cookies.json");
    this.credentials = {
      username: process.env.FORUM_USERNAME || "",
      password: process.env.FORUM_PASSWORD || "",
    };
    this.mode = process.env.NODE_ENV || "";
    this.s3Service = new S3Service();

    // Validate node configuration
    if (this.NODE_INDEX < 0 || this.NODE_INDEX >= this.NODE_COUNT) {
      throw new Error(
        `Invalid NODE_INDEX: ${this.NODE_INDEX}. Must be between 0 and ${
          this.NODE_COUNT - 1
        }`
      );
    }

    console.log(
      `🚀 Starting scraper instance: Node ${this.NODE_INDEX}/${
        this.NODE_COUNT - 1
      }`
    );
  }

  /**
   * Helper method to determine if a URL is an image
   */
  public isImageUrl(url: string): boolean {
    const imageExtensions = [
      ".jpg",
      ".jpeg",
      ".png",
      ".gif",
      ".bmp",
      ".webp",
      ".svg",
    ];
    const lowerUrl = url.toLowerCase();
    return imageExtensions.some((ext) => lowerUrl.includes(ext));
  }

  /**
   * Helper method to determine if a URL is a video
   */
  private isVideoUrl(url: string): boolean {
    const videoExtensions = [
      ".mp4",
      ".avi",
      ".mov",
      ".wmv",
      ".flv",
      ".webm",
      ".mkv",
    ];
    const lowerUrl = url.toLowerCase();
    return (
      videoExtensions.some((ext) => lowerUrl.includes(ext)) ||
      lowerUrl.includes("youtube.com") ||
      lowerUrl.includes("youtu.be") ||
      lowerUrl.includes("vimeo.com")
    );
  }

  /**
   * Helper method to determine if a URL is an image or video
   */
  private isImageOrVideo(url: string): boolean {
    return this.isImageUrl(url) || this.isVideoUrl(url);
  }

  /**
   * Helper method to determine if a URL is a thumbnail
   */
  private isThumbnailUrl(url: string): boolean {
    return url.endsWith("_thumb");
  }

  /**
   * Helper method to filter out tracking pixels and unwanted URLs
   */
  private shouldSkipUrl(url: string): boolean {
    // Filter out 1x1 transparent pixel GIF (common tracking pixel)
    if (
      url.includes(
        "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
      )
    ) {
      return true;
    }

    // Filter out other common tracking pixel patterns
    if (
      url.includes("data:image/gif;base64,") &&
      url.includes("R0lGODlhAQABAIAAAAAAAP")
    ) {
      return true;
    }

    // Filter out invalid URLs
    if (url === "#" || url === "javascript:void(0)") {
      return true;
    }

    return false;
  }

  /**
   * Filter out unwanted URLs from post media arrays
   */
  private filterPostMedias(posts: PostData[]): PostData[] {
    return posts.map((post) => ({
      ...post,
      medias: post.medias.filter(([fullUrl, thumbUrl]) => {
        return !this.shouldSkipUrl(fullUrl) && !this.shouldSkipUrl(thumbUrl);
      }),
    }));
  }

  /**
   * Helper method to determine if a URL is NOT a raw image file
   * (i.e., it's an attachment page that needs to be processed)
   */
  public isNotRawImg(url: string): boolean {
    const lowerUrl = url.toLowerCase();

    // Check if it has image name but no extension (like screenshot-2024-01-25-013402-png.120147661)
    const imageNames = ["jpg", "jpeg", "png", "gif", "bmp", "webp", "svg"];
    const hasImageName = imageNames.some(
      (name) =>
        lowerUrl.includes(`-${name}`) ||
        lowerUrl.includes(`_${name}`) ||
        lowerUrl.includes(`${name}-`) ||
        lowerUrl.includes(name)
    );

    // Check if it's a direct image file with extension
    const imageExtensions = [
      ".jpg",
      ".jpeg",
      ".png",
      ".gif",
      ".bmp",
      ".webp",
      ".svg",
    ];
    const hasImageExtension = imageExtensions.some((ext) =>
      lowerUrl.includes(ext)
    );

    // Check for video extensions as well
    const videoExtensions = [
      ".mp4",
      ".avi",
      ".mov",
      ".wmv",
      ".flv",
      ".webm",
      ".mkv",
    ];
    const hasVideoExtension = videoExtensions.some((ext) =>
      lowerUrl.includes(ext)
    );

    // Check for video names without extension
    const videoNames = ["mp4", "avi", "mov", "wmv", "flv", "webm", "mkv"];
    const hasVideoName = videoNames.some(
      (name) =>
        lowerUrl.includes(`-${name}`) ||
        lowerUrl.includes(`_${name}`) ||
        lowerUrl.includes(`${name}-`) ||
        lowerUrl.includes(name)
    );

    // Return true if it has image/video name but no extension (attachment page)
    return (
      (hasImageName || hasVideoName) && !hasImageExtension && !hasVideoExtension
    );
  }

  /**
   * Download image from LPSG attachment page by extracting the actual CDN URL
   * @param attachmentUrl - The attachment page URL
   * @returns Promise<{buffer: Buffer, extension: string} | null> - The image buffer and extension or null if failed
   */
  private async downloadFromAttachmentPage(
    attachmentUrl: string
  ): Promise<{ buffer: Buffer; extension: string } | null> {
    const MAX_RETRIES = 3;
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      let attachmentPage: Page | null = null;

      try {
        if (attempt > 1) {
          console.log(
            `Retry attempt ${attempt}/${MAX_RETRIES} for attachment: ${attachmentUrl}`
          );
          // Add delay between retries
          await this.delay(1000 * attempt); // Progressive delay: 1s, 2s, 3s
        }

        // console.log(`Downloading from attachment page: ${attachmentUrl}`);

        if (!this.browser) {
          throw new Error("Browser not initialized");
        }

        // Extract file extension from the attachment URL
        const fileExtension =
          this.extractFileExtensionFromAttachmentUrl(attachmentUrl);

        // Create a new page for this attachment
        attachmentPage = await this.browser.newPage();

        // Set user agent for the new page
        await attachmentPage.setUserAgent(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        );

        // Copy cookies from the main page to ensure authentication
        if (this.page) {
          const cookies = await this.page.cookies();
          await attachmentPage.setCookie(...cookies);
          // console.log(`Copied ${cookies.length} cookies to attachment page`);
        }

        // Navigate to the attachment page
        await attachmentPage.goto(attachmentUrl, {
          waitUntil: "networkidle2",
          timeout: 30000,
        });

        // Wait a bit for the page to fully load
        await this.delay(500);

        // Wait for the image to be fully loaded on the page
        await attachmentPage.waitForSelector("img", { timeout: 10000 });

        // Download the image using the page's context to maintain authentication
        // Navigate to the image URL directly to get the image data
        const response = await attachmentPage.goto(attachmentUrl, {
          waitUntil: "networkidle2",
          timeout: 30000,
        });

        if (!response || !response.ok()) {
          throw new Error(`Failed to load image: ${response?.status()}`);
        }

        // Get the response body as buffer
        const imageBuffer = await response.buffer();

        // Close the attachment page before returning success
        if (attachmentPage) {
          await attachmentPage.close();
        }

        return { buffer: imageBuffer, extension: fileExtension };
      } catch (error) {
        lastError = error as Error;
        console.error(
          `❌ Error downloading from attachment page ${attachmentUrl} (attempt ${attempt}/${MAX_RETRIES}):`
        );

        // Close the attachment page on error
        if (attachmentPage) {
          try {
            await attachmentPage.close();
          } catch (closeError) {
            console.error("Error closing attachment page:", closeError);
          }
        }

        // If this is the last attempt, don't continue
        if (attempt === MAX_RETRIES) {
          break;
        }
      }
    }

    // All retries failed
    console.error(
      `Failed to download attachment after ${MAX_RETRIES} attempts: ${attachmentUrl}`
    );
    return null;
  }

  /**
   * Extract file extension from attachment URL
   * @param attachmentUrl - The attachment page URL
   * @returns string - The file extension (e.g., '.png', '.jpg')
   */
  public extractFileExtensionFromAttachmentUrl(attachmentUrl: string): string {
    const lowerUrl = attachmentUrl.toLowerCase();

    // Check for common image extensions in the URL (with dot)
    const imageExtensions = [
      ".png",
      ".jpg",
      ".jpeg",
      ".gif",
      ".bmp",
      ".webp",
      ".svg",
    ];

    for (const ext of imageExtensions) {
      if (lowerUrl.includes(ext)) {
        return ext;
      }
    }

    // Check for extensions without dot (like "-gif", "-png", etc.)
    const extensionsWithoutDot = [
      "png",
      "jpg",
      "jpeg",
      "gif",
      "bmp",
      "webp",
      "svg",
    ];

    for (const ext of extensionsWithoutDot) {
      // Look for patterns like "-gif", "_gif", or "gif-" in the URL
      if (
        lowerUrl.includes(`-${ext}`) ||
        lowerUrl.includes(`_${ext}`) ||
        lowerUrl.includes(`${ext}-`)
      ) {
        return `.${ext}`;
      }
    }

    // Check for video extensions as well
    const videoExtensions = [
      ".mp4",
      ".avi",
      ".mov",
      ".wmv",
      ".flv",
      ".webm",
      ".mkv",
    ];

    for (const ext of videoExtensions) {
      if (lowerUrl.includes(ext)) {
        return ext;
      }
    }

    // Check for video extensions without dot
    const videoExtensionsWithoutDot = [
      "mp4",
      "avi",
      "mov",
      "wmv",
      "flv",
      "webm",
      "mkv",
    ];

    for (const ext of videoExtensionsWithoutDot) {
      if (
        lowerUrl.includes(`-${ext}`) ||
        lowerUrl.includes(`_${ext}`) ||
        lowerUrl.includes(`${ext}-`)
      ) {
        return `.${ext}`;
      }
    }
    return ".jpg";
  }

  private async initializeBrowser(): Promise<{
    browser: Browser;
    tempDir: string;
  }> {
    const tempDir = createTempUserDataDir();
    const browserConfig = createBrowserConfig(this.mode, tempDir);

    const browser = await puppeteer.launch(browserConfig);
    console.log("Browser initialized");

    return { browser, tempDir };
  }

  /**
   * Check available memory and reboot system if below threshold in production
   */
  private async checkMemoryAndRebootIfNeeded(): Promise<void> {
    if (this.mode !== "production") {
      return; // Only check in production mode
    }

    try {
      const { exec } = require("child_process");
      const util = require("util");
      const execAsync = util.promisify(exec);

      // Get memory info using free command
      const { stdout } = await execAsync("free -m");
      const lines = stdout.split("\n");
      const memLine = lines[1]; // Second line contains memory info

      if (memLine) {
        const parts = memLine.split(/\s+/);
        const availableMemoryMB = parseInt(parts[6]); // Available memory in MB

        console.log(
          `💾 Available memory: ${availableMemoryMB}MB (threshold: ${this.MIN_AVAILABLE_MEMORY_MB}MB)`
        );

        if (availableMemoryMB < this.MIN_AVAILABLE_MEMORY_MB) {
          console.log(
            `🚨 CRITICAL: Available memory (${availableMemoryMB}MB) is below threshold (${this.MIN_AVAILABLE_MEMORY_MB}MB)`
          );
          console.log(`🔄 Initiating system reboot in 5 seconds...`);

          // Give time for logs to be written
          await this.delay(5000);

          // Execute reboot command
          console.log(`🔄 Executing system reboot...`);
          await execAsync("sudo reboot");
        }
      }
    } catch (error) {
      console.error("Error checking memory or rebooting:", error);
    }
  }

  /**
   * Start memory monitoring in production mode
   */
  private startMemoryMonitoring(): void {
    if (this.mode !== "production") {
      return; // Only monitor in production mode
    }

    console.log(
      `🔍 Starting memory monitoring (checking every ${
        this.MEMORY_CHECK_INTERVAL / 1000
      }s, threshold: ${this.MIN_AVAILABLE_MEMORY_MB}MB)`
    );

    this.stopMemoryMonitoring();

    this.memoryCheckInterval = setInterval(async () => {
      await this.checkMemoryAndRebootIfNeeded();
    }, this.MEMORY_CHECK_INTERVAL);
  }

  /**
   * Stop memory monitoring
   */
  private stopMemoryMonitoring(): void {
    if (this.memoryCheckInterval) {
      clearInterval(this.memoryCheckInterval);
      this.memoryCheckInterval = null;
      console.log("🔍 Memory monitoring stopped");
    }
  }

  async initialize(): Promise<void> {
    const { browser, tempDir } = await this.initializeBrowser();
    this.browser = browser;
    this.tempDir = tempDir;

    this.page = await this.browser.newPage();

    // Clear cache and cookies for production mode
    if (this.mode === "production") {
      await clearBrowserCache(this.page);
      await clearSystemCaches();
    }

    await this.page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    );

    // Ensure logged in before starting
    const isLoggedIn = await this.ensureLoggedIn();
    if (!isLoggedIn) {
      throw new Error("Failed to login");
    }

    // Start memory monitoring in production mode
    this.startMemoryMonitoring();
  }

  async loadCookies(): Promise<boolean> {
    try {
      if (fs.existsSync(this.cookiesPath)) {
        const cookies = JSON.parse(fs.readFileSync(this.cookiesPath, "utf8"));
        await this.page!.setCookie(...cookies);
        return true;
      }
    } catch (error) {
      // console.log("No valid cookies found or error loading cookies:", error);
    }
    return false;
  }

  async saveCookies(): Promise<void> {
    try {
      const cookies = await this.page!.cookies();
      fs.writeFileSync(this.cookiesPath, JSON.stringify(cookies, null, 2));
    } catch (error) {
      console.error("Error saving cookies:", error);
    }
  }

  async handleCookieConsent(): Promise<void> {
    try {
      // Check if cookie consent button exists
      const cookieButton = await this.page!.$(
        'a[href*="/misc/cookies"][class*="button--notice"]'
      );

      if (cookieButton) {
        await cookieButton.click();
      }
    } catch (error) {
      // console.log(
      //   "No cookie consent dialog found or error handling it:",
      //   error
      // );
    }
  }

  async checkLoginStatus(): Promise<boolean> {
    try {
      await this.page!.goto(this.FORUM_URL, {
        waitUntil: "networkidle2",
        timeout: 5000,
      });

      // Handle cookie consent first
      await this.handleCookieConsent();

      // Check if user account link exists (means we're logged in)
      const userAccountLink = await this.page!.$('a[href="/account/"]');
      return !!userAccountLink; // If user account link exists, we're logged in
    } catch (error) {
      return false;
    }
  }

  async login(): Promise<boolean> {
    try {
      // Navigate to the forum page first
      await this.page!.goto(this.FORUM_URL, {
        waitUntil: "networkidle2",
      });

      // Handle cookie consent first
      await this.handleCookieConsent();

      // Wait a bit for the page to fully load
      await this.delay(2000);

      // Try multiple selectors for the login link
      let loginLink = await this.page!.$('a[href="/login/"]');

      if (!loginLink) {
        // Try alternative selector
        loginLink = await this.page!.$("a.p-navgroup-link--logIn");
      }

      if (!loginLink) {
        // Try clicking by text content using evaluate - FIXED: Handle properly
        const loginLinkFound = await this.page!.evaluate(() => {
          const document = (globalThis as any).document;
          const links = Array.from(document.querySelectorAll("a"));
          const loginLink = links.find((link: any) =>
            link.textContent?.includes("Log in")
          );
          if (loginLink) {
            (loginLink as any).click();
            return true;
          }
          return false;
        });

        if (!loginLinkFound) {
          return true; // Already logged in or no login link found
        }
      } else {
        // We have a proper ElementHandle, check visibility and click normally
        try {
          const isVisible = await loginLink.isVisible();
          if (!isVisible) {
            await loginLink.scrollIntoView();
            await this.delay(1000);
          }
          await loginLink.click();
        } catch (clickError) {
          // Fallback: try clicking via evaluate
          await this.page!.evaluate((element) => {
            element.click();
          }, loginLink);
        }
      }

      // Wait a bit for modal to start opening
      await this.delay(2000);

      // Try multiple selectors for the login form
      let loginForm = await this.page!.$('input[name="login"]');

      if (!loginForm) {
        // Try other possible selectors
        loginForm = await this.page!.$('input[type="text"]');
        if (!loginForm) {
          loginForm = await this.page!.$('input[placeholder*="name"]');
        }
        if (!loginForm) {
          loginForm = await this.page!.$('input[placeholder*="email"]');
        }
        if (!loginForm) {
          loginForm = await this.page!.$('input[placeholder*="username"]');
        }
      }

      if (!loginForm) {
        await this.page!.screenshot({ path: "debug-login-detail.png" });
        return false;
      }

      // Add a small delay to ensure modal is fully loaded
      await this.delay(1000);

      // Clear any existing text and fill in credentials
      await this.page!.focus('input[name="login"]');
      await this.page!.keyboard.down("Control");
      await this.page!.keyboard.press("KeyA");
      await this.page!.keyboard.up("Control");
      await this.page!.type('input[name="login"]', this.credentials.username);

      await this.page!.focus('input[name="password"]');
      await this.page!.keyboard.down("Control");
      await this.page!.keyboard.press("KeyA");
      await this.page!.keyboard.up("Control");
      await this.page!.type(
        'input[name="password"]',
        this.credentials.password
      );

      // Try multiple selectors for the submit button
      let submitButton = await this.page!.$(
        'button[type="submit"].button--primary.button--icon--login'
      );

      if (!submitButton) {
        submitButton = await this.page!.$(
          'button.button--primary[type="submit"]'
        );
      }

      if (!submitButton) {
        submitButton = await this.page!.$('button[type="submit"]');
      }

      if (submitButton) {
        await submitButton.click();
      } else {
        return false;
      }

      // Wait for the modal to close and page to update
      await this.delay(3000);

      // Check if login was successful by looking for user account link
      const userAccountLink = await this.page!.$('a[href="/account/"]');

      if (userAccountLink) {
        await this.saveCookies();
        return true;
      } else {
        return false;
      }
    } catch (error) {
      console.error("Error during login:", error);
      return false;
    }
  }

  async ensureLoggedIn(): Promise<boolean> {
    // Try to load existing cookies first
    const cookiesLoaded = await this.loadCookies();

    if (cookiesLoaded) {
      // Check if we're still logged in with the cookies
      const isLoggedIn = await this.checkLoginStatus();
      if (isLoggedIn) {
        // console.log("Already logged in with saved cookies");
        return true;
      }
    }

    // If not logged in, attempt login
    return await this.login();
  }

  /**
   * Get threads needing update - filtered by node distribution
   */
  async getThreadsNeedingUpdate(): Promise<ForumThread[]> {
    try {
      // Get all threads where detailPageUpdateDate is null OR lastReplyDate > detailPageUpdateDate
      const allThreads = await ForumThread.findAll({
        where: sequelize.or(
          { detailPageUpdateDate: null },
          sequelize.where(
            sequelize.col("lastReplyDate"),
            ">",
            sequelize.col("detailPageUpdateDate")
          )
        ),
        order: [["lastReplyDate", "DESC"]],
      });

      console.log(
        `Found ${allThreads.length} total threads needing detail page updates`
      );

      // Filter threads based on node distribution using modulo operation
      const nodeThreads = allThreads.filter((thread) => {
        // threadId is already a number
        return thread.threadId % this.NODE_COUNT === this.NODE_INDEX;
      });

      console.log(
        `Node ${this.NODE_INDEX}/${this.NODE_COUNT - 1}: Processing ${
          nodeThreads.length
        } threads (${((nodeThreads.length / allThreads.length) * 100).toFixed(
          1
        )}% of total)`
      );

      return nodeThreads;
    } catch (error) {
      console.error("Error getting threads needing update:", error);
      return [];
    }
  }

  async scrapeThreadDetailPage(thread: ForumThread): Promise<void> {
    try {
      console.log(
        `Node ${this.NODE_INDEX}/${
          this.NODE_COUNT - 1
        }: Scraping detail page for thread: ${thread.threadId}`
      );

      // Clean and construct the URL
      let cleanUrl = thread.threadUrl;

      // Skip if it's a forum URL (not a thread URL)
      // if (cleanUrl.includes("/forums/") || cleanUrl.includes("?prefix_id=")) {
      //   console.log(`Skipping forum URL: ${cleanUrl}`);
      //   return;
      // }

      // Remove /unread suffix if present
      if (cleanUrl.endsWith("/unread")) {
        cleanUrl = cleanUrl.replace("/unread", "");
      }

      // Ensure URL ends with /
      if (!cleanUrl.endsWith("/")) {
        cleanUrl += "/";
      }

      const fullUrl = `${this.SITE_URL}${cleanUrl}`;

      // Get total pages for this thread
      const totalPages = await this.getTotalPages(fullUrl);

      // Scrape pages in ASCENDING order (first to last)
      // Start from lastUpdatedPage + 1, or page 1 if lastUpdatedPage is null
      const startPage = thread.lastUpdatedPage ? thread.lastUpdatedPage : 1;
      let shouldContinueScraping = true;

      for (
        let pageNum = startPage;
        pageNum <= totalPages && shouldContinueScraping;
        pageNum++
      ) {
        console.log(
          `Node ${this.NODE_INDEX}/${
            this.NODE_COUNT - 1
          }: Scraping page ${pageNum} of ${totalPages} (starting from page ${startPage})...`
        );

        // Handle URL structure: /threads/title.id/page-X or just /threads/title.id/ for page 1
        const pageUrl = pageNum === 1 ? fullUrl : `${fullUrl}page-${pageNum}`;
        console.log(`Page URL: ${pageUrl}`);

        // Retry logic: try loading the page up to 3 times
        let pageLoadSuccess = false;
        const MAX_RETRIES = 3;

        for (
          let attempt = 1;
          attempt <= MAX_RETRIES && !pageLoadSuccess;
          attempt++
        ) {
          try {
            if (attempt > 1) {
              console.log(
                `Retry attempt ${attempt}/${MAX_RETRIES} for page ${pageNum}`
              );
            }

            // Add 30-second timeout for page loading
            await Promise.race([
              this.page!.goto(pageUrl, {
                waitUntil: "networkidle2",
              }),
              new Promise((_, reject) =>
                setTimeout(
                  () => reject(new Error("Page load timeout after 30 seconds")),
                  30000
                )
              ),
            ]);

            // Handle cookie consent on first page loaded
            if (pageNum === startPage && attempt === 1) {
              await this.handleCookieConsent();
            }

            const posts = await this.scrapePagePosts();

            // Check which posts already exist in database
            const postIds = posts.map((post) => post.postId);
            const existingPosts = await ForumPost.findAll({
              where: {
                threadId: thread.threadId,
                postId: { [Op.in]: postIds },
              },
              attributes: ["postId"],
            });

            const existingPostIds = new Set(
              existingPosts.map((post) => post.postId)
            );

            // Filter out posts that already exist
            const newPosts = posts.filter(
              (post) => !existingPostIds.has(post.postId)
            );

            console.log(
              `Page ${pageNum}: Total posts: ${posts.length}, Existing: ${existingPosts.length}, New: ${newPosts.length}`
            );

            // If ALL posts already exist, stop scraping (we've reached old content)
            if (newPosts.length === 0 && postIds.length !== 0) {
              shouldContinueScraping = false;
              break;
            }

            // Save only new posts to database with batch processing
            await this.savePostsToDatabase(thread.threadId, newPosts);

            // Update lastUpdatedPage after successful page scraping
            await ForumThread.update(
              { lastUpdatedPage: pageNum },
              { where: { threadId: thread.threadId } }
            );

            // Clear memory cache after each page is processed
            await this.clearPageMemoryCache();

            // Increment pages scraped counter
            this.pagesScraped++;

            // Check if we need to restart browser
            if (this.pagesScraped >= this.PAGES_BEFORE_RESTART) {
              await this.restartBrowser();
            }

            // Mark page load as successful
            pageLoadSuccess = true;
          } catch (error) {
            console.error(
              `Error on page ${pageNum}, attempt ${attempt}/${MAX_RETRIES}: ${error}`
            );

            // If this is not the last attempt, restart browser and retry
            if (attempt < MAX_RETRIES) {
              console.log(`Restarting browser before retry...`);
              await this.restartBrowser();
              await this.delay(2000); // Wait 2 seconds before retry
            } else {
              // Last attempt failed, restart browser and skip entire thread
              console.error(
                `Failed to load page ${pageNum} after ${MAX_RETRIES} attempts. Skipping entire thread ${thread.threadId}.`
              );
              await this.restartBrowser();
              this.pagesScraped++;
              return; // Exit the entire thread scraping
            }
          }
        }
      }

      // Update detailPageUpdateDate to match lastReplier field after all pages are done
      await ForumThread.update(
        { detailPageUpdateDate: thread.lastReplyDate },
        { where: { threadId: thread.threadId } }
      );

      console.log(
        `Completed scraping thread ${thread.threadId} - detailPageUpdateDate set to: ${thread.lastReplyDate}`
      );
    } catch (error) {
      console.error(`Error scraping thread ${thread.threadId}:`, error);
    }
  }

  private async getTotalPages(threadUrl: string): Promise<number> {
    try {
      await this.page!.goto(threadUrl, { waitUntil: "networkidle2" });

      const totalPages = await this.page!.evaluate(() => {
        const document = (globalThis as any).document;
        const pageNav = document.querySelector(".pageNav-main");
        if (!pageNav) return 1;

        const lastPageLink = pageNav.querySelector("li:last-child a");
        if (!lastPageLink) return 1;

        const href = lastPageLink.getAttribute("href");
        // Match pattern like "page-3" in URLs like "/threads/mc-kolos.3654511/page-3"
        const match = href.match(/page-(\d+)$/);
        return match ? parseInt(match[1]) : 1;
      });

      return totalPages;
    } catch (error) {
      console.error("Error getting total pages:", error);
      return 1;
    }
  }

  /**
   * Scrape all posts from current page and collect all media URLs
   * UPDATED: Extract full-size image URLs from <a href> links instead of thumbnails
   */
  private async scrapePagePosts(): Promise<PostData[]> {
    try {
      const posts = await this.page!.evaluate(() => {
        const document = (globalThis as any).document;
        const postElements = document.querySelectorAll(".message");
        const posts: PostData[] = [];

        postElements.forEach((element: any) => {
          try {
            // Extract post ID from article element - get the numeric ID
            const articleElement = element.closest("article[data-content]");
            let postId = 0;

            if (articleElement) {
              const contentAttr = articleElement.getAttribute("data-content");
              if (contentAttr) {
                const match = contentAttr.match(/post-(\d+)/);
                if (match) {
                  postId = parseInt(match[1]);
                }
              }
            }

            // Fallback methods if article element not found
            if (!postId) {
              const dataLbId = element.getAttribute("data-lb-id");
              if (dataLbId) {
                postId = parseInt(dataLbId) || 0;
              }
            }

            if (!postId) {
              const elementId = element.id;
              if (elementId) {
                const match = elementId.match(/js-post-(\d+)/);
                if (match) {
                  postId = parseInt(match[1]);
                }
              }
            }

            // Extract author
            const authorElement = element.querySelector(
              ".message-userDetails .username"
            );
            const author = authorElement?.textContent?.trim() || "";

            // Extract content
            const contentElement = element.querySelector(
              ".message-content .bbWrapper"
            );
            const content = contentElement?.textContent?.trim() || "";

            // Extract post created date from time element's title attribute
            let postCreatedDate = "";
            const timeElement = element.querySelector("time.u-dt[title]");
            if (timeElement) {
              const title = timeElement.getAttribute("title");
              if (title) {
                // Parse the title format "Oct 5, 2025 at 5:03 PM"
                // and convert to "2024-01-24T19:34:34-0500" format
                const datetimeAttr = timeElement.getAttribute("datetime");
                if (datetimeAttr) {
                  // Use the datetime attribute which is already in ISO format
                  postCreatedDate = datetimeAttr;
                }
              }
            }

            // Extract likes count
            // Format: "cutthefx, XanRodck25, Synth1984 and 8 others"
            // Or: "cutthefx and 5K others" (can have K, M, B suffixes)
            // Or: "cutthefx" (just one person)
            let likes = "0";
            const reactionsLink = element.querySelector(
              '.reactionsBar-link[href*="/reactions"]'
            );
            if (reactionsLink) {
              const text = reactionsLink.textContent || "";

              // Check for "and X others" pattern (X can be like "8", "5K", "1.5M", etc.)
              const othersMatch = text.match(
                /and\s+([0-9.]+)([KMB]?)\s+others?/i
              );

              if (othersMatch) {
                const otherCount = othersMatch[1];
                const suffix = othersMatch[2];

                if (suffix) {
                  // If there's a K/M/B suffix, use that value as-is (it's approximate total)
                  likes = otherCount + suffix;
                } else {
                  // If plain number, add visible names count
                  const nameElements = reactionsLink.querySelectorAll("bdi");
                  const totalLikes = nameElements.length + parseInt(otherCount);
                  likes = totalLikes.toString();
                }
              } else {
                // No "others" text, just count visible names (bdi tags)
                const nameElements = reactionsLink.querySelectorAll("bdi");
                likes = nameElements.length.toString();
              }
            }

            // Extract media (images and videos) - UPDATED LOGIC
            // Each media is a pair: [fullImageUrl, thumbImageUrl] where thumb can be empty
            const medias: [string, string][] = [];

            // Get attachments from message-attachments section
            const attachmentSection = element.querySelector(
              ".message-attachments"
            );
            if (attachmentSection) {
              // Get attachment links (full-size images) - PRIORITY: Extract from <a href>
              const attachmentLinks = attachmentSection.querySelectorAll(
                'a[href*="/attachments/"]'
              );
              attachmentLinks.forEach((link: any) => {
                const href = link.getAttribute("href");
                if (href) {
                  const fullUrl = href.startsWith("http")
                    ? href
                    : `https://www.lpsg.com${href}`;

                  // Check if this <a> tag contains an <img> (thumbnail)
                  const imgInsideLink = link.querySelector("img[src]");
                  if (imgInsideLink) {
                    // If there's a thumbnail inside the link, add both full and thumb
                    const imgSrc = imgInsideLink.getAttribute("src");
                    if (imgSrc) {
                      const thumbUrl = imgSrc.startsWith("http")
                        ? imgSrc
                        : `https://www.lpsg.com${imgSrc}`;

                      // Add pair: [fullUrl, thumbUrl]
                      medias.push([fullUrl, thumbUrl]);
                    }
                  } else {
                    // No thumbnail found, add full URL with empty thumb
                    medias.push([fullUrl, ""]);
                  }
                }
              });
            }

            // Get images from message content (inline images)
            const contentImages = element.querySelectorAll(
              ".message-content img[src], .message-content img[data-src]"
            );
            contentImages.forEach((img: any) => {
              // Skip if this image is inside .message-attachments section (already processed)
              if (img.closest(".message-attachments")) {
                return;
              }

              // Try to get src first, then data-src
              let src = img.getAttribute("src") || img.getAttribute("data-src");

              if (
                src &&
                !src.includes("avatar") &&
                !src.includes("smiley") &&
                !src.includes("icon")
              ) {
                // Convert relative URLs to absolute
                const fullUrl = src.startsWith("http")
                  ? src
                  : `https://www.lpsg.com${src}`;

                // Regular inline image with empty thumb
                medias.push([fullUrl, ""]);
              }
            });

            // Get videos from message content
            // const videoElements = element.querySelectorAll(
            //   ".message-content video source, .message-content video[src]"
            // );
            // videoElements.forEach((video: any) => {
            //   const src = video.getAttribute("src");
            //   if (src) {
            //     const fullUrl = src.startsWith("http")
            //       ? src
            //       : `https://www.lpsg.com${src}`;
            //     // Add video with empty thumb
            //     medias.push([fullUrl, ""]);
            //   }
            // });

            // Remove duplicates from media pairs
            // Keep only unique pairs based on full image URL
            const urlMap = new Map<string, [string, string]>();

            for (const [fullUrl, thumbUrl] of medias) {
              // Use full URL as key for deduplication (or thumb URL if full is empty)
              const keyUrl = fullUrl || thumbUrl;

              // If we haven't seen this URL before, add the pair
              if (!urlMap.has(keyUrl)) {
                urlMap.set(keyUrl, [fullUrl, thumbUrl]);
              }
            }

            const uniqueMedias = Array.from(urlMap.values());

            if (postId) {
              posts.push({
                postId,
                author,
                content,
                postCreatedDate,
                likes,
                medias: uniqueMedias,
              });
            }
          } catch (error) {
            console.error("Error parsing post element:", error);
          }
        });

        return posts;
      });

      // Filter out tracking pixels and unwanted URLs from the scraped posts
      const filteredPosts = this.filterPostMedias(posts);

      return filteredPosts;
    } catch (error) {
      console.error("Error scraping page posts:", error);
      return [];
    }
  }

  /**
   * Process media for a batch of posts (30 posts per batch)
   * Downloads and uploads all media in parallel for better performance
   */
  private async processBatchMedia(
    posts: PostData[],
    threadId: number
  ): Promise<Map<number, Array<{ s3Url: string; hasThumbnail: boolean }>>> {
    const postMediaMap = new Map<
      number,
      Array<{ s3Url: string; hasThumbnail: boolean }>
    >();

    // Collect all media tasks for the batch
    const allMediaTasks: Array<{
      postId: number;
      fullSizeUrl: string;
      thumbUrl: string;
      fullKey: string;
      thumbKey: string;
    }> = [];

    // Prepare all media tasks
    for (const post of posts) {
      let uniqueId = 0;

      for (const [fullUrl, thumbUrl] of post.medias) {
        if (fullUrl || thumbUrl) {
          // Skip if either URL is a tracking pixel or unwanted
          if (this.shouldSkipUrl(fullUrl) || this.shouldSkipUrl(thumbUrl)) {
            continue;
          }

          const baseUrl = fullUrl || thumbUrl;
          let fullKey: string;
          let thumbKey: string;
          let extension: string | undefined;

          if (this.isNotRawImg(baseUrl)) {
            extension = this.extractFileExtensionFromAttachmentUrl(baseUrl);

            fullKey = this.s3Service.generateKeyWithExtension(
              baseUrl,
              threadId,
              post.postId,
              extension,
              false,
              uniqueId
            );

            thumbKey = this.s3Service.generateKeyWithExtension(
              baseUrl,
              threadId,
              post.postId,
              extension,
              true,
              uniqueId
            );
          } else {
            fullKey = this.s3Service.generateKey(
              baseUrl,
              threadId,
              post.postId,
              false,
              uniqueId
            );

            thumbKey = this.s3Service.generateKey(
              baseUrl,
              threadId,
              post.postId,
              true,
              uniqueId
            );
          }

          allMediaTasks.push({
            postId: post.postId,
            fullSizeUrl: fullUrl || "",
            thumbUrl: thumbUrl || "",
            fullKey,
            thumbKey,
          });
        }

        uniqueId++;
      }
    }

    if (allMediaTasks.length === 0) {
      return postMediaMap;
    }

    console.log(`Processing ${allMediaTasks.length} media files in batch...`);

    // Create individual upload tasks
    const uploadTasks: Array<{
      postId: number;
      url: string;
      key: string;
      isThumb: number;
      hasThumb: boolean;
    }> = [];

    for (const task of allMediaTasks) {
      const hasThumb = !!task.thumbUrl;

      if (task.fullSizeUrl) {
        uploadTasks.push({
          postId: task.postId,
          url: task.fullSizeUrl,
          key: task.fullKey,
          isThumb: 0,
          hasThumb: hasThumb,
        });
      }

      if (task.thumbUrl) {
        uploadTasks.push({
          postId: task.postId,
          url: task.thumbUrl,
          key: task.thumbKey,
          isThumb: 1,
          hasThumb: hasThumb,
        });
      }
    }

    // Process all upload tasks in parallel
    const processingPromises = uploadTasks.map(async (uploadTask) => {
      try {
        let buffer: Buffer | null = null;

        if (this.isNotRawImg(uploadTask.url)) {
          const result = await this.downloadFromAttachmentPage(uploadTask.url);
          if (result) {
            buffer = result.buffer;
          }
        } else {
          buffer = await this.s3Service.downloadFile(uploadTask.url);
        }

        if (buffer) {
          const contentType = this.s3Service.getContentType(uploadTask.url);
          const uploadCommand = new PutObjectCommand({
            Bucket: this.s3Service.bucketName,
            Key: uploadTask.key,
            Body: buffer,
            ContentType: contentType,
            Metadata: {
              "original-url": uploadTask.url,
              "upload-timestamp": new Date().toISOString(),
              "thread-id": threadId.toString(),
              "post-id": uploadTask.postId.toString(),
              "is-thumb": uploadTask.isThumb.toString(),
              "has-thumb": uploadTask.hasThumb.toString(),
            },
          });

          await this.s3Service.s3Client.send(uploadCommand);
          const finalS3Url = `https://${this.s3Service.bucketName}.s3.${
            process.env.S3_REGION || "us-east-2"
          }.wasabisys.com/${uploadTask.key}`;

          console.log(
            `✓ Uploaded: ${uploadTask.url} -> ${finalS3Url} (thumb: ${uploadTask.isThumb})`
          );
          return {
            postId: uploadTask.postId,
            s3Url: finalS3Url,
            isThumb: uploadTask.isThumb,
            hasThumb: uploadTask.hasThumb,
            success: true,
          };
        } else {
          console.error(`✗ Failed to download: ${uploadTask.url}`);
          return {
            postId: uploadTask.postId,
            s3Url: uploadTask.url,
            isThumb: uploadTask.isThumb,
            hasThumb: uploadTask.hasThumb,
            success: false,
          };
        }
      } catch (error) {
        console.error(`✗ Processing failed for ${uploadTask.url}:`, error);
        return {
          postId: uploadTask.postId,
          s3Url: uploadTask.url,
          isThumb: uploadTask.isThumb,
          hasThumb: uploadTask.hasThumb,
          success: false,
        };
      }
    });

    const results = await Promise.allSettled(processingPromises);

    // Group results by post ID
    for (const result of results) {
      if (result.status === "fulfilled" && result.value) {
        const { postId, s3Url, isThumb, success, hasThumb } = result.value;

        if (success && isThumb === 0) {
          if (!postMediaMap.has(postId)) {
            postMediaMap.set(postId, []);
          }
          postMediaMap.get(postId)!.push({ s3Url, hasThumbnail: hasThumb });
        }
      }
    }
    return postMediaMap;
  }

  /**
   * Save posts to database with batch processing (30 posts per batch)
   * No S3 deletion - only uploads new media
   */
  private async savePostsToDatabase(
    threadId: number,
    posts: PostData[]
  ): Promise<void> {
    try {
      const BATCH_SIZE = 5;
      let totalProcessed = 0;

      await ForumMedia.destroy({
        where: {
          threadId: threadId,
          postId: {
            [Op.in]: posts.map((post) => post.postId),
          },
          type: "img",
        },
      });

      // Process posts in batches
      for (let i = 0; i < posts.length; i += BATCH_SIZE) {
        const batch = posts.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(posts.length / BATCH_SIZE);

        // Process media for this batch in parallel
        const postMediaMap = await this.processBatchMedia(batch, threadId);

        // Save posts and media to database
        const dbPromises: Promise<any>[] = [];

        // Save/update posts (without medias field)
        batch.forEach((postData) => {
          dbPromises.push(
            ForumPost.upsert({
              postId: postData.postId,
              threadId: threadId,
              author: postData.author,
              content: postData.content,
              postCreatedDate: postData.postCreatedDate,
              likes: postData.likes,
            })
          );
        });

        // Save media data to ForumMedia table
        for (const postData of batch) {
          const processedMedias = postMediaMap.get(postData.postId) || [];

          for (const mediaData of processedMedias) {
            const { s3Url, hasThumbnail } = mediaData;

            if (s3Url.includes("s3.") && s3Url.includes("wasabisys.com")) {
              const mediaType = this.isImageUrl(s3Url)
                ? "img"
                : this.isVideoUrl(s3Url)
                ? "mov"
                : null;

              if (mediaType && !s3Url.includes("_thumb")) {
                dbPromises.push(
                  ForumMedia.create({
                    threadId: threadId,
                    postId: postData.postId,
                    link: s3Url,
                    type: mediaType,
                    existThumb: hasThumbnail ? 1 : 0,
                  })
                );
              }
            }
          }
        }

        await Promise.allSettled(dbPromises);

        // Log media count for each post in this batch
        for (const postData of batch) {
          const processedMedias = postMediaMap.get(postData.postId) || [];
          const imageCount = processedMedias.filter(
            (media) =>
              media.s3Url.includes("s3.") &&
              media.s3Url.includes("wasabisys.com") &&
              !media.s3Url.includes("_thumb") &&
              this.isImageUrl(media.s3Url)
          ).length;
          const videoCount = processedMedias.filter(
            (media) =>
              media.s3Url.includes("s3.") &&
              media.s3Url.includes("wasabisys.com") &&
              !media.s3Url.includes("_thumb") &&
              this.isVideoUrl(media.s3Url)
          ).length;

          console.log(
            `Post ID ${postData.postId}: ${imageCount} images, ${videoCount} videos`
          );
        }

        totalProcessed += batch.length;

        // Clear memory after each batch to prevent memory accumulation
        await this.clearPageMemoryCache();
      }

      console.log(
        `✓ Completed processing all ${totalProcessed} posts for thread ${threadId}`
      );
    } catch (error) {
      console.error("Error saving posts to database:", error);
    }
  }

  async clearBrowserCache(): Promise<void> {
    try {
      if (!this.page) return;

      console.log("Clearing browser cache and memory...");

      // Clear browser cache
      const client = await this.page.target().createCDPSession();
      await client.send("Network.clearBrowserCache");
      await client.send("Network.clearBrowserCookies");

      // Clear memory
      await client.send("Runtime.evaluate", {
        expression: `
          if (window.gc) {
            window.gc();
          }
          // Clear any cached data
          if (window.caches) {
            caches.keys().then(names => {
              names.forEach(name => caches.delete(name));
            });
          }
        `,
      });

      // Force garbage collection if available
      await client.send("HeapProfiler.collectGarbage");

      await client.detach();

      // Clear system-level cache for production mode
      if (this.mode === "production") {
        await clearSystemCaches();
      }

      console.log("Browser cache and memory cleared successfully");
    } catch (error) {
      console.error("Error clearing browser cache:", error);
    }
  }

  /**
   * Clear memory cache after each page is processed
   * Lightweight memory cleanup to prevent accumulation
   */
  async clearPageMemoryCache(): Promise<void> {
    try {
      if (!this.page) return;

      console.log("Clearing page memory cache...");

      // Clear browser cache
      const client = await this.page.target().createCDPSession();
      await client.send("Network.clearBrowserCache");

      // Clear memory and force garbage collection
      await client.send("Runtime.evaluate", {
        expression: `
          if (window.gc) {
            window.gc();
          }
          // Clear any cached data
          if (window.caches) {
            caches.keys().then(names => {
              names.forEach(name => caches.delete(name));
            });
          }
        `,
      });

      // Force garbage collection
      await client.send("HeapProfiler.collectGarbage");

      await client.detach();

      // Force Node.js garbage collection if available
      if (global.gc) {
        global.gc();
      }

      console.log("Page memory cache cleared successfully");
    } catch (error) {
      console.error("Error clearing page memory cache:", error);
    }
  }

  async restartBrowser(): Promise<void> {
    try {
      console.log("Restarting browser...");

      // Show memory usage before restart
      await getMemoryUsage();

      // Close current browser
      if (this.browser) {
        await this.browser.close();
        this.browser = null;
        this.page = null;
      }

      // Clean up temp directory
      if (this.tempDir) {
        await deleteTempUserDataDir(this.mode);
      }

      // Reset page counter
      this.pagesScraped = 0;

      // Reinitialize browser (this will include cache clearing in production)
      await this.initialize();

      // Show memory usage after restart
      await getMemoryUsage();

      console.log("Browser restarted successfully");
    } catch (error) {
      console.error("Error restarting browser:", error);
      throw error;
    }
  }

  private async delay(ms: number): Promise<void> {
    return delay(ms);
  }

  async close(): Promise<void> {
    // Stop memory monitoring
    this.stopMemoryMonitoring();

    if (this.browser) {
      await this.browser.close();
      console.log("Detail page scraper closed");
    }
  }

  /**
   * Scrape ALL pages of a thread (including existing posts)
   * This method scrapes every page and every post, regardless of whether they already exist
   * @param thread - The thread to scrape all pages for
   */
  async scrapeAllThreadPages(thread: ForumThread): Promise<void> {
    try {
      console.log(
        `Node ${this.NODE_INDEX}/${
          this.NODE_COUNT - 1
        }: Scraping ALL pages for thread: ${thread.threadId}`
      );

      // Clean and construct the URL
      let cleanUrl = thread.threadUrl;

      // Remove /unread suffix if present
      if (cleanUrl.endsWith("/unread")) {
        cleanUrl = cleanUrl.replace("/unread", "");
      }

      // Ensure URL ends with /
      if (!cleanUrl.endsWith("/")) {
        cleanUrl += "/";
      }

      const fullUrl = `${this.SITE_URL}${cleanUrl}`;

      // Get total pages for this thread
      const totalPages = await this.getTotalPages(fullUrl);

      console.log(`Thread has ${totalPages} total pages`);

      // Scrape pages in ASCENDING order (first to last)
      // Start from lastUpdatedPage + 1, or page 1 if lastUpdatedPage is null
      const startPage = thread.lastUpdatedPage ? thread.lastUpdatedPage : 1;

      for (let pageNum = startPage; pageNum <= totalPages; pageNum++) {
        console.log(
          `Node ${this.NODE_INDEX}/${
            this.NODE_COUNT - 1
          }: Scraping page ${pageNum} of ${totalPages} (ALL POSTS MODE, starting from page ${startPage})...`
        );

        // Handle URL structure: /threads/title.id/page-X or just /threads/title.id/ for page 1
        const pageUrl = pageNum === 1 ? fullUrl : `${fullUrl}page-${pageNum}`;
        console.log(`Page URL: ${pageUrl}`);

        // Retry logic: try loading the page up to 3 times
        let pageLoadSuccess = false;
        const MAX_RETRIES = 5;

        for (
          let attempt = 1;
          attempt <= MAX_RETRIES && !pageLoadSuccess;
          attempt++
        ) {
          try {
            if (attempt > 1) {
              console.log(
                `Retry attempt ${attempt}/${MAX_RETRIES} for page ${pageNum}`
              );
            }

            // Add 30-second timeout for page loading
            await Promise.race([
              this.page!.goto(pageUrl, {
                waitUntil: "networkidle2",
              }),
              new Promise((_, reject) =>
                setTimeout(
                  () => reject(new Error("Page load timeout after 30 seconds")),
                  30000
                )
              ),
            ]);

            // Handle cookie consent on first page loaded
            if (pageNum === startPage && attempt === 1) {
              await this.handleCookieConsent();
            }

            const posts = await this.scrapePagePosts();

            console.log(
              `Page ${pageNum}: Found ${posts.length} posts (ALL POSTS MODE - processing all)`
            );

            // Save ALL posts to database (no filtering for existing posts)
            await this.saveAllPostsToDatabase(thread.threadId, posts);

            // Update lastUpdatedPage after successful page scraping
            await ForumThread.update(
              { lastUpdatedPage: pageNum },
              { where: { threadId: thread.threadId } }
            );

            // Clear memory cache after each page is processed
            await this.clearPageMemoryCache();

            // Increment pages scraped counter
            this.pagesScraped++;

            // Check if we need to restart browser
            if (this.pagesScraped >= this.PAGES_BEFORE_RESTART) {
              await this.restartBrowser();
            }

            // Mark page load as successful
            pageLoadSuccess = true;
          } catch (error) {
            console.error(
              `Error on page ${pageNum}, attempt ${attempt}/${MAX_RETRIES}: ${error}`
            );

            // If this is not the last attempt, restart browser and retry
            if (attempt < MAX_RETRIES) {
              console.log(`Restarting browser before retry...`);
              await this.restartBrowser();
              await this.delay(2000); // Wait 2 seconds before retry
            } else {
              // Last attempt failed, restart browser and skip entire thread
              console.error(
                `Failed to load page ${pageNum} after ${MAX_RETRIES} attempts. Skipping entire thread ${thread.threadId}.`
              );
              await this.restartBrowser();
              this.pagesScraped++;
              return; // Exit the entire thread scraping
            }
          }
        }
      }

      // Update detailPageUpdateDate to match lastReplier field after all pages are done
      await ForumThread.update(
        { detailPageUpdateDate: thread.lastReplyDate },
        { where: { threadId: thread.threadId } }
      );

      console.log(
        `Completed scraping ALL pages for thread ${thread.threadId} - detailPageUpdateDate set to: ${thread.lastReplyDate}`
      );
    } catch (error) {
      console.error(
        `Error scraping all pages for thread ${thread.threadId}:`,
        error
      );
    }
  }

  /**
   * Save ALL posts to database (including existing ones) with batch processing
   * This method processes all posts without checking for existing ones
   * @param threadId - The thread ID
   * @param posts - All posts to save
   */
  private async saveAllPostsToDatabase(
    threadId: number,
    posts: PostData[]
  ): Promise<void> {
    try {
      const BATCH_SIZE = 5;
      let totalProcessed = 0;

      // Delete existing media for these posts first (to avoid duplicates)
      await ForumMedia.destroy({
        where: {
          threadId: threadId,
          postId: {
            [Op.in]: posts.map((post) => post.postId),
          },
        },
      });

      // Process posts in batches
      for (let i = 0; i < posts.length; i += BATCH_SIZE) {
        const batch = posts.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(posts.length / BATCH_SIZE);

        console.log(
          `Processing batch ${batchNum}/${totalBatches} (${batch.length} posts) - ALL POSTS MODE`
        );

        // Process media for this batch in parallel
        const postMediaMap = await this.processBatchMedia(batch, threadId);

        // Save posts and media to database
        const dbPromises: Promise<any>[] = [];

        // Save/update ALL posts (without medias field)
        batch.forEach((postData) => {
          dbPromises.push(
            ForumPost.upsert({
              postId: postData.postId,
              threadId: threadId,
              author: postData.author,
              content: postData.content,
              postCreatedDate: postData.postCreatedDate,
              likes: postData.likes,
            })
          );
        });

        // Save media data to ForumMedia table
        for (const postData of batch) {
          const processedMedias = postMediaMap.get(postData.postId) || [];

          for (const mediaData of processedMedias) {
            const { s3Url, hasThumbnail } = mediaData;

            if (s3Url.includes("s3.") && s3Url.includes("wasabisys.com")) {
              const mediaType = this.isImageUrl(s3Url)
                ? "img"
                : this.isVideoUrl(s3Url)
                ? "mov"
                : null;

              if (mediaType && !s3Url.includes("_thumb")) {
                dbPromises.push(
                  ForumMedia.create({
                    threadId: threadId,
                    postId: postData.postId,
                    link: s3Url,
                    type: mediaType,
                    existThumb: hasThumbnail ? 1 : 0,
                  })
                );
              }
            }
          }
        }

        await Promise.allSettled(dbPromises);

        // Log media count for each post in this batch
        for (const postData of batch) {
          const processedMedias = postMediaMap.get(postData.postId) || [];
          const imageCount = processedMedias.filter(
            (media) =>
              media.s3Url.includes("s3.") &&
              media.s3Url.includes("wasabisys.com") &&
              !media.s3Url.includes("_thumb") &&
              this.isImageUrl(media.s3Url)
          ).length;
          const videoCount = processedMedias.filter(
            (media) =>
              media.s3Url.includes("s3.") &&
              media.s3Url.includes("wasabisys.com") &&
              !media.s3Url.includes("_thumb") &&
              this.isVideoUrl(media.s3Url)
          ).length;

          console.log(
            `Post ID ${postData.postId}: ${imageCount} images, ${videoCount} videos`
          );
        }

        totalProcessed += batch.length;

        // Clear memory after each batch to prevent memory accumulation
        await this.clearPageMemoryCache();
      }

      console.log(
        `✓ Completed processing ALL ${totalProcessed} posts for thread ${threadId} (ALL POSTS MODE)`
      );
    } catch (error) {
      console.error("Error saving all posts to database:", error);
    }
  }

  /**
   * Run detail page scraping for a specific thread by threadId
   * @param threadId - The ID of the thread to scrape
   * @returns Promise<ForumThread | null> - The scraped thread data or null if not found
   */
  async runDetailPage(threadId: number): Promise<ForumThread | null> {
    try {
      console.log(`Starting detail page scraping for thread ID: ${threadId}`);

      // Initialize browser and login
      await this.initialize();

      // Find the thread by ID
      const thread = await ForumThread.findOne({
        where: { threadId: threadId },
      });

      if (!thread) {
        console.error(`Thread with ID ${threadId} not found in database`);
        return null;
      }

      console.log(`Found thread: ${thread.title} (${thread.threadId})`);

      // Scrape the thread detail page
      await this.scrapeAllThreadPages(thread);
      // await this.scrapeThreadDetailPage(thread);

      console.log(
        `Successfully completed detail page scraping for thread ${threadId}`
      );

      // Return the updated thread data
      return await ForumThread.findOne({
        where: { threadId: threadId },
      });
    } catch (error) {
      console.error(`Error in runDetailPage for thread ${threadId}:`, error);
      return null;
    } finally {
      await this.close();
    }
  }

  async run(): Promise<void> {
    try {
      await this.initialize();

      const threadsToUpdate = await this.getThreadsNeedingUpdate();

      console.log(
        `Node ${this.NODE_INDEX}: Processing ${threadsToUpdate.length} threads`
      );

      if (threadsToUpdate.length === 0) {
        console.log(`Node ${this.NODE_INDEX}: No threads to process`);
        return;
      }

      let processedCount = 0;
      for (const thread of threadsToUpdate) {
        processedCount++;
        console.log(
          `Node ${this.NODE_INDEX}: Processing thread ${processedCount}/${threadsToUpdate.length} - ${thread.title}`
        );

        // await this.scrapeThreadDetailPage(thread);
        await this.scrapeAllThreadPages(thread);

        // Add delay between threads
        await this.delay(1000);
      }

      console.log(
        `Node ${this.NODE_INDEX}: Detail page scraping completed - processed ${processedCount} threads`
      );
    } catch (error) {
      console.error(
        `Node ${this.NODE_INDEX}: Error in detail page scraping:`,
        error
      );
    } finally {
      await this.close();
    }
  }
}

export { ForumDetailPageScraper };
