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
import { deleteS3ImagesByThreadAndPost } from "./s3FileList";
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
dotenv.config();

export interface PostData {
  postId: number;
  author: string;
  content: string;
  postCreatedDate: string;
  likes: number;
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
   * Helper method to determine if a URL is NOT a raw image file
   * (i.e., it's an attachment page that needs to be processed)
   */
  public isNotRawImg(url: string): boolean {
    const lowerUrl = url.toLowerCase();

    // Check if it has image name but no extension (like screenshot-2024-01-25-013402-png.120147661)
    const imageNames = ["jpg", "jpeg", "png", "gif", "bmp", "webp", "svg"];
    const hasImageName = imageNames.some((name) => lowerUrl.includes(name));

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

    // Return true if it has image name but no extension (attachment page)
    return hasImageName && !hasImageExtension;
  }

  /**
   * Download image from LPSG attachment page by extracting the actual CDN URL
   * @param attachmentUrl - The attachment page URL
   * @returns Promise<{buffer: Buffer, extension: string} | null> - The image buffer and extension or null if failed
   */
  private async downloadFromAttachmentPage(
    attachmentUrl: string
  ): Promise<{ buffer: Buffer; extension: string } | null> {
    let attachmentPage: Page | null = null;

    try {
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
      return { buffer: imageBuffer, extension: fileExtension };
    } catch (error) {
      // console.error(`Error downloading from attachment page ${attachmentUrl}:`);
      return null;
    } finally {
      // Always close the attachment page to prevent memory leaks
      if (attachmentPage) {
        try {
          await attachmentPage.close();
        } catch (closeError) {
          console.error("Error closing attachment page:", closeError);
        }
      }
    }
  }

  /**
   * Extract file extension from attachment URL
   * @param attachmentUrl - The attachment page URL
   * @returns string - The file extension (e.g., '.png', '.jpg')
   */
  public extractFileExtensionFromAttachmentUrl(attachmentUrl: string): string {
    const lowerUrl = attachmentUrl.toLowerCase();

    // Check for common image extensions in the URL
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

    // Default to .jpg if no extension found
    // console.warn(
    //   `No file extension found in URL: ${attachmentUrl}, defaulting to .jpg`
    // );
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
      console.log(`Node ${this.NODE_INDEX}/${this.NODE_COUNT - 1}: Scraping detail page for thread: ${thread.threadId}`);

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
      console.log(`Thread has ${totalPages} pages`);

      // Scrape all pages and save after each page
      for (let pageNum = 1; pageNum <= totalPages; pageNum++) {
        console.log(`Node ${this.NODE_INDEX}/${this.NODE_COUNT - 1}: Scraping page ${pageNum} of ${totalPages}...`);

        // Handle URL structure: /threads/title.id/page-X or just /threads/title.id/ for page 1
        const pageUrl = pageNum === 1 ? fullUrl : `${fullUrl}page-${pageNum}`;
        console.log(`Page URL: ${pageUrl}`);

        try {
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

          // Handle cookie consent on first page only
          if (pageNum === 1) {
            await this.handleCookieConsent();
          }

          const posts = await this.scrapePagePosts();

          // Save posts to database with batch processing
          await this.savePostsToDatabase(thread.threadId, posts);

          // Clear memory cache after each page is processed
          await this.clearPageMemoryCache();

          // Increment pages scraped counter
          this.pagesScraped++;

          // Check if we need to restart browser
          if (this.pagesScraped >= this.PAGES_BEFORE_RESTART) {
            // console.log(
            //   `Reached ${this.PAGES_BEFORE_RESTART} pages scraped. Restarting browser...`
            // );
            await this.restartBrowser();
          }
        } catch (error) {
          // console.error(
          //   `❌ Page ${pageNum} failed to load within 30 seconds: ${pageUrl}`
          // );
          console.error(`Error: ${error}`);

          // Skip this page and continue to next page
          // console.log(
          //   `⏭️    page ${pageNum} failed to load. Restarting browser...`
          // );
          await this.restartBrowser();

          this.pagesScraped++;
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
            let likes = 0;
            const reactionsLink = element.querySelector(
              '.reactionsBar-link[href*="/reactions"]'
            );
            if (reactionsLink) {
              const text = reactionsLink.textContent || "";
              
              // Check for "and X other person/people" pattern
              const otherMatch = text.match(/and\s+(\d+)\s+other/);
              
              if (otherMatch) {
                // Count named people (split by comma) + "and X others"
                const namedPeople = text.split(",").length;
                const otherCount = parseInt(otherMatch[1]);
                likes = namedPeople + otherCount;
              } else {
                // No "and X others", just count comma-separated names
                likes = text.split(",").length;
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
            const videoElements = element.querySelectorAll(
              ".message-content video source, .message-content video[src]"
            );
            videoElements.forEach((video: any) => {
              const src = video.getAttribute("src");
              if (src) {
                const fullUrl = src.startsWith("http")
                  ? src
                  : `https://www.lpsg.com${src}`;
                // Add video with empty thumb
                medias.push([fullUrl, ""]);
              }
            });

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

      console.log(`Scraped ${posts.length} posts`);

      return posts;
    } catch (error) {
      console.error("Error scraping page posts:", error);
      return [];
    }
  }

  /**
   * Process all media from a page in streaming batches - UPDATED: Always process posts
   * Downloads a batch, uploads immediately, clears memory, then repeats
   */
  private async processPageMedia(
    posts: PostData[],
    threadId: number
  ): Promise<Map<number, Array<{ s3Url: string; hasThumbnail: boolean }>>> {
    const allPosts = posts;
    const postMediaMap = new Map<
      number,
      Array<{ s3Url: string; hasThumbnail: boolean }>
    >();

    // Collect all media tasks for the entire page
    const allMediaTasks: Array<{
      postId: number;
      fullSizeUrl: string;
      thumbUrl: string;
      fullKey: string;
      thumbKey: string;
    }> = [];

    // Prepare all media tasks with unique IDs per post
    for (const post of allPosts) {
      let uniqueId = 0; // Reset unique ID for each post

      for (const [fullUrl, thumbUrl] of post.medias) {
        // Only process if we have at least one URL (full or thumb)
        if (fullUrl || thumbUrl) {
          // Generate keys for both full and thumb images
          const baseUrl = fullUrl || thumbUrl;
          let fullKey: string;
          let thumbKey: string;
          let extension: string | undefined;

          if (this.isNotRawImg(baseUrl)) {
            // For attachment pages, we need to extract the extension
            extension = this.extractFileExtensionFromAttachmentUrl(baseUrl);

            // Generate full image key
            fullKey = this.s3Service.generateKeyWithExtension(
              baseUrl,
              threadId,
              post.postId,
              extension,
              false, // Full image
              uniqueId
            );

            // Generate thumbnail key
            thumbKey = this.s3Service.generateKeyWithExtension(
              baseUrl,
              threadId,
              post.postId,
              extension,
              true, // Thumbnail
              uniqueId
            );
          } else {
            // For regular images, use the existing method

            // Generate full image key
            fullKey = this.s3Service.generateKey(
              baseUrl,
              threadId,
              post.postId,
              false, // Full image
              uniqueId
            );

            // Generate thumbnail key
            thumbKey = this.s3Service.generateKey(
              baseUrl,
              threadId,
              post.postId,
              true, // Thumbnail
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

        uniqueId++; // Increment unique ID for next media in this post
      }
    }

    console.log(`Processing ${allMediaTasks.length} media files for page...`);

    // Create individual upload tasks for full and thumb images
    const uploadTasks: Array<{
      postId: number;
      url: string;
      key: string;
      isThumb: number;
      hasThumb: boolean;
    }> = [];

    for (const task of allMediaTasks) {
      // Check if this media pair has a thumbnail
      const hasThumb = !!task.thumbUrl;

      // Add full image task if URL exists
      if (task.fullSizeUrl) {
        uploadTasks.push({
          postId: task.postId,
          url: task.fullSizeUrl,
          key: task.fullKey, // Full image key
          isThumb: 0,
          hasThumb: hasThumb,
        });
      }

      // Add thumbnail task if URL exists
      if (task.thumbUrl) {
        uploadTasks.push({
          postId: task.postId,
          url: task.thumbUrl,
          key: task.thumbKey, // Thumbnail key
          isThumb: 1,
          hasThumb: hasThumb,
        });
      }
    }

    // Process all upload tasks in parallel
    const processingPromises = uploadTasks.map(async (uploadTask) => {
      try {
        let buffer: Buffer | null = null;

        // Check if it's an attachment page that needs special handling
        if (this.isNotRawImg(uploadTask.url)) {
          const result = await this.downloadFromAttachmentPage(uploadTask.url);
          if (result) {
            buffer = result.buffer;
          }
        } else {
          // Regular image download
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
            originalUrl: uploadTask.url,
            s3Url: finalS3Url,
            isThumb: uploadTask.isThumb,
            hasThumb: uploadTask.hasThumb,
            success: true,
          };
        } else {
          console.error(`✗ Failed to download: ${uploadTask.url}`);
          return {
            postId: uploadTask.postId,
            originalUrl: uploadTask.url,
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
          originalUrl: uploadTask.url,
          s3Url: uploadTask.url,
          isThumb: uploadTask.isThumb,
          hasThumb: uploadTask.hasThumb,
          success: false,
        };
      }
    });

    // Wait for all processing to complete
    const results = await Promise.allSettled(processingPromises);

    // Group results by post ID and track thumbnail existence
    const postResults = new Map<
      number,
      Array<{ s3Url: string; hasThumbnail: boolean }>
    >();

    for (const result of results) {
      if (result.status === "fulfilled" && result.value) {
        const { postId, s3Url, isThumb, success, hasThumb } = result.value;

        // Only process full images (existThumb: 0) and successful uploads
        if (success && isThumb === 0) {
          if (!postResults.has(postId)) {
            postResults.set(postId, []);
          }
          postResults.get(postId)!.push({ s3Url, hasThumbnail: hasThumb });
        }
      }
    }

    console.log(`✓ Completed processing media files for page`);
    return postResults;
  }

  /**
   * UPDATED savePostsToDatabase method - Save posts and media data separately
   */
  private async savePostsToDatabase(
    threadId: number,
    posts: PostData[]
  ): Promise<void> {
    try {
      // Process all media - UPDATED: Process ALL posts
      const postMediaMap = await this.processPageMedia(posts, threadId);

      // UPDATED: Process ALL posts, not just new ones
      const allPosts = posts;

      console.log(`Processing ${allPosts.length} posts for thread ${threadId}`);

      // Save posts and media separately
      const dbPromises: Promise<any>[] = [];

      // Save/update posts (without medias field)
      allPosts.forEach((postData) => {
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

      // Save media data to ForumMedia table - only for successfully uploaded S3 files
      for (const postData of allPosts) {
        const processedMedias = postMediaMap.get(postData.postId) || [];

        // Delete existing S3 image files for this post before saving new ones
        try {
          await deleteS3ImagesByThreadAndPost(threadId, postData.postId);
        } catch (error) {
          console.error(
            `Failed to delete S3 images for thread ${threadId}, post ${postData.postId}:`,
            error
          );
        }

        // Delete existing media records from database for this post before saving new ones
        dbPromises.push(
          ForumMedia.destroy({
            where: {
              postId: postData.postId,
              threadId: threadId,
              type: "img",
            },
          })
        );

        for (const mediaData of processedMedias) {
          const { s3Url, hasThumbnail } = mediaData;

          // Only save media that is an S3 bucket URL (successfully uploaded)
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
                  type: mediaType, // Always use the original media type (img/mov)
                  existThumb: hasThumbnail ? 1 : 0, // 1 if thumbnail exists, 0 if not
                })
              );
            }
          }
        }
      }

      await Promise.allSettled(dbPromises);

      console.log(
        `✓ Completed processing ${allPosts.length} posts for thread ${threadId}`
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
      await this.scrapeThreadDetailPage(thread);

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

        await this.scrapeThreadDetailPage(thread);

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
