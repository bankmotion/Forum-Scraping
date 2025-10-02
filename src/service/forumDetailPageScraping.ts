import puppeteer, { Browser, Page, LaunchOptions } from "puppeteer";
import { ForumThread } from "../model/ForumThread";
import { ForumPost } from "../model/ForumPost";
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
dotenv.config();

interface PostData {
  postId: number;
  author: string;
  content: string;
  medias: string[];
}

interface MediaTask {
  url: string;
  postId: number;
  threadId: string;
  key: string;
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

  // Batch processing configuration
  private readonly BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "30");
  private readonly DOWNLOAD_BATCH_SIZE = parseInt(
    process.env.DOWNLOAD_BATCH_SIZE || "30"
  );
  private readonly UPLOAD_BATCH_SIZE = parseInt(
    process.env.UPLOAD_BATCH_SIZE || "30"
  );

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
  private isImageUrl(url: string): boolean {
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
        // Convert threadId to number for modulo operation
        const threadIdNum = parseInt(thread.threadId);
        return threadIdNum % this.NODE_COUNT === this.NODE_INDEX;
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
      console.log(`Scraping detail page for thread: ${thread.title}`);

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
      console.log(`Full URL: ${fullUrl}`);

      // Get total pages for this thread
      const totalPages = await this.getTotalPages(fullUrl);
      console.log(`Thread has ${totalPages} pages`);

      // Scrape all pages and save after each page
      for (let pageNum = 1; pageNum <= totalPages; pageNum++) {
        console.log(`Scraping page ${pageNum} of ${totalPages}...`);

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
        const posts: any[] = [];

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

            // Extract media (images and videos) - UPDATED LOGIC
            const medias: string[] = [];

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
                  medias.push(fullUrl);
                }
              });

              if (medias.length === 0) {
                // Get attachment images (thumbnails) - SECONDARY: Only if no <a href> found
                const attachmentImages =
                  attachmentSection.querySelectorAll("img[src]");
                attachmentImages.forEach((img: any) => {
                  const src = img.getAttribute("src");
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
                    medias.push(fullUrl);
                  }
                });
              }
            }

            // Get images from message content (inline images)
            const contentImages = element.querySelectorAll(
              ".message-content img[src]"
            );
            contentImages.forEach((img: any) => {
              const src = img.getAttribute("src");
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
                medias.push(fullUrl);
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
                medias.push(fullUrl);
              }
            });

            // Get embedded videos (YouTube, etc.) from message content
            const embedElements = element.querySelectorAll(
              ".message-content iframe[src]"
            );
            embedElements.forEach((iframe: any) => {
              const src = iframe.getAttribute("src");
              if (src) {
                medias.push(src);
              }
            });

            // Get any other attachment links in the message
            const allAttachmentLinks = element.querySelectorAll(
              'a[href*="/attachments/"]'
            );
            allAttachmentLinks.forEach((link: any) => {
              const href = link.getAttribute("href");
              if (href) {
                const fullUrl = href.startsWith("http")
                  ? href
                  : `https://www.lpsg.com${href}`;
                medias.push(fullUrl);
              }
            });

            // Remove duplicates
            const uniqueMedias = [...new Set(medias)];

            if (postId && author && content) {
              posts.push({
                postId,
                author,
                content,
                medias: uniqueMedias,
              });
            }
          } catch (error) {
            console.error("Error parsing post element:", error);
          }
        });

        return posts;
      });

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
  private async processPageMediaBatch(
    posts: PostData[],
    threadId: string,
    existingPostIds: Set<number>
  ): Promise<Map<number, string[]>> {
    console.log(
      `Processing media for ${posts.length} posts in thread ${threadId}`
    );

    // UPDATED: Always process all posts, not just new ones
    const allPosts = posts; // Process all posts, even existing ones

    console.log(
      `Processing ${allPosts.length} posts (including existing ones) for thread ${threadId}`
    );

    // Collect all media tasks from ALL posts
    const allMediaTasks: MediaTask[] = [];
    const postMediaMap = new Map<number, string[]>();

    for (const post of allPosts) {
      const processedMedias: string[] = [];

      for (const mediaUrl of post.medias) {
        if (this.isImageUrl(mediaUrl)) {
          const key = this.s3Service.generateKey(
            mediaUrl,
            threadId,
            post.postId
          );
          allMediaTasks.push({
            url: mediaUrl,
            postId: post.postId,
            threadId: threadId,
            key: key,
          });
          processedMedias.push(mediaUrl); // Keep original URL for now
        }
      }

      postMediaMap.set(post.postId, processedMedias);
    }

    console.log(
      `Found ${allMediaTasks.length} media files to process from ${allPosts.length} posts`
    );

    if (allMediaTasks.length === 0) {
      return postMediaMap;
    }

    // Process in streaming batches: download -> upload -> clear memory -> repeat
    const uploadResults = await this.processStreamingBatches(allMediaTasks);

    // Update post media map with S3 URLs
    for (const [postId, originalUrls] of postMediaMap.entries()) {
      const updatedUrls: string[] = [];

      for (const originalUrl of originalUrls) {
        const uploadResult = uploadResults.get(originalUrl);
        if (uploadResult && uploadResult.success) {
          updatedUrls.push(uploadResult.s3Url!);
        } else {
          // Keep original URL if upload failed
          updatedUrls.push(originalUrl);
        }
      }

      postMediaMap.set(postId, updatedUrls);
    }

    return postMediaMap;
  }

  /**
   * Process media in streaming batches to prevent memory overflow
   * Downloads a batch, uploads immediately, clears memory, then repeats
   */
  private async processStreamingBatches(
    mediaTasks: MediaTask[]
  ): Promise<Map<string, { success: boolean; s3Url?: string }>> {
    const uploadResults = new Map<
      string,
      { success: boolean; s3Url?: string }
    >();

    console.log(
      `Starting streaming batch process (${this.BATCH_SIZE} files per batch)...`
    );

    let i = 0;
    let batchNumber = 1;

    while (i < mediaTasks.length) {
      // Create batch of 5 files (or remaining files if less than 5)
      const batch = mediaTasks.slice(i, i + this.BATCH_SIZE);
      const totalBatches = Math.ceil(mediaTasks.length / this.BATCH_SIZE);

      console.log(
        `Processing streaming batch ${batchNumber}/${totalBatches} (${batch.length} files)`
      );

      // Step 1: Download batch
      const downloadResults = new Map<string, Buffer>();
      let totalDownloadedSize = 0;

      const downloadPromises = batch.map(async (task) => {
        try {
          const buffer = await this.s3Service.downloadFile(task.url);
          downloadResults.set(task.url, buffer);
          totalDownloadedSize += buffer.length;
          console.log(
            `✓ Downloaded: ${task.url} (${(buffer.length / 1024 / 1024).toFixed(
              1
            )}MB)`
          );
        } catch (error) {
          console.error(`✗ Download failed: ${task.url}`, error);
        }
      });

      await Promise.allSettled(downloadPromises);

      console.log(
        `📊 Batch total size: ${(totalDownloadedSize / 1024 / 1024).toFixed(
          1
        )}MB`
      );

      // Step 2: Upload batch immediately
      const uploadPromises = batch.map(async (task) => {
        try {
          const buffer = downloadResults.get(task.url);
          if (!buffer) {
            console.error(`✗ No buffer found for: ${task.url}`);
            uploadResults.set(task.url, { success: false });
            return;
          }

          const contentType = this.s3Service.getContentType(task.url);

          const uploadCommand = new PutObjectCommand({
            Bucket: this.s3Service.bucketName,
            Key: task.key,
            Body: buffer,
            ContentType: contentType,
            Metadata: {
              "original-url": task.url,
              "upload-timestamp": new Date().toISOString(),
              "thread-id": task.threadId,
              "post-id": task.postId.toString(),
            },
          });

          await this.s3Service.s3Client.send(uploadCommand);

          const s3Url = `https://${this.s3Service.bucketName}.s3.${process.env.S3_REGION}.wasabisys.com/${task.key}`;
          uploadResults.set(task.url, { success: true, s3Url });
          console.log(`✓ Uploaded: ${task.url} -> ${s3Url}`);
        } catch (error) {
          console.error(`✗ Upload failed: ${task.url}`, error);
          uploadResults.set(task.url, { success: false });
        }
      });

      await Promise.allSettled(uploadPromises);

      // Step 3: Clear memory immediately after upload
      downloadResults.clear();

      // Force garbage collection if available
      if (global.gc) {
        global.gc();
      }

      console.log(
        `✓ Completed streaming batch ${batchNumber}/${totalBatches} - memory cleared (${(
          totalDownloadedSize /
          1024 /
          1024
        ).toFixed(1)}MB processed)`
      );

      // Move to next batch
      i += this.BATCH_SIZE;
      batchNumber++;

      // Small delay between batches to allow memory cleanup
      if (i < mediaTasks.length) {
        await this.delay(1000);
      }
    }

    const successCount = Array.from(uploadResults.values()).filter(
      (r) => r.success
    ).length;
    console.log(
      `Streaming batch process completed: ${successCount}/${mediaTasks.length} files processed successfully`
    );

    return uploadResults;
  }

  /**
   * UPDATED savePostsToDatabase method - Always process posts and replace only image URLs
   */
  private async savePostsToDatabase(
    threadId: string,
    posts: PostData[]
  ): Promise<void> {
    try {
      // Get all existing posts for this thread
      const existingPosts = await ForumPost.findAll({
        where: { threadId: threadId },
        attributes: ["postId", "medias"],
      });

      // Create a map of existing post IDs for quick lookup
      const existingPostIds = new Set(existingPosts.map((post) => post.postId));

      console.log(
        `Found ${existingPostIds.size} existing posts for thread ${threadId}`
      );

      // Process all media in batches - UPDATED: Process ALL posts
      const postMediaMap = await this.processPageMediaBatch(
        posts,
        threadId,
        existingPostIds
      );

      // UPDATED: Process ALL posts, not just new ones
      const allPosts = posts;

      console.log(`Processing ${allPosts.length} posts for thread ${threadId}`);

      // Save ALL posts to database after batch processing is complete
      const dbPromises = allPosts.map(async (postData) => {
        const processedMedias = postMediaMap.get(postData.postId) || [];

        // UPDATED: For existing posts, replace only image URLs, preserve video URLs
        if (existingPostIds.has(postData.postId)) {
          const existingPost = existingPosts.find(
            (p) => p.postId === postData.postId
          );
          if (existingPost) {
            const existingMedias = JSON.parse(existingPost.medias || "[]");
            const newImageUrls = processedMedias.filter((url) =>
              this.isImageUrl(url)
            );
            const existingVideoUrls = existingMedias.filter((url: string) =>
              this.isVideoUrl(url)
            );

            // Combine new image URLs with existing video URLs
            const finalMedias = [...newImageUrls, ...existingVideoUrls];

            await ForumPost.upsert({
              postId: postData.postId,
              threadId: threadId,
              author: postData.author,
              content: postData.content,
              medias: JSON.stringify(finalMedias),
            });
            console.log(
              `✓ Updated post ${postData.postId} with ${newImageUrls.length} new image URLs, preserved ${existingVideoUrls.length} video URLs`
            );
          }
        } else {
          // New posts: save all media
          if (processedMedias.length > 0) {
            await ForumPost.upsert({
              postId: postData.postId,
              threadId: threadId,
              author: postData.author,
              content: postData.content,
              medias: JSON.stringify(processedMedias),
            });
            console.log(
              `✓ Saved new post ${postData.postId} with ${processedMedias.length} media files`
            );
          }
        }
      });

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
        await this.delay(3000);
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
