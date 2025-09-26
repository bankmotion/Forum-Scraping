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
  delay
} from "../utils";
dotenv.config();

interface PostData {
  postId: number;
  author: string;
  content: string;
  medias: string[];
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
  private readonly PAGES_BEFORE_RESTART: number = 100;
  private tempDir: string = "";

  constructor() {
    this.cookiesPath = path.join(__dirname, "../../cookies.json");
    this.credentials = {
      username: process.env.FORUM_USERNAME || "",
      password: process.env.FORUM_PASSWORD || "",
    };
    this.mode = process.env.NODE_ENV || "";
    this.s3Service = new S3Service();
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
  }

  async loadCookies(): Promise<boolean> {
    try {
      if (fs.existsSync(this.cookiesPath)) {
        const cookies = JSON.parse(fs.readFileSync(this.cookiesPath, "utf8"));
        await this.page!.setCookie(...cookies);
        return true;
      }
    } catch (error) {
      console.log("No valid cookies found or error loading cookies:", error);
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
        await this.delay(1000); // Wait for dialog to close
      }
    } catch (error) {
      console.log(
        "No cookie consent dialog found or error handling it:",
        error
      );
    }
  }

  async checkLoginStatus(): Promise<boolean> {
    try {
      await this.page!.goto(this.FORUM_URL, {
        waitUntil: "networkidle2",
      });

      // Handle cookie consent first
      await this.handleCookieConsent();

      // Check if user account link exists (means we're logged in)
      const userAccountLink = await this.page!.$('a[href="/account/"]');
      return !!userAccountLink; // If user account link exists, we're logged in
    } catch (error) {
      console.error("Error checking login status:", error);
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
        // Try clicking by text content using evaluate
        loginLink = await this.page!.evaluateHandle(() => {
          const document = (globalThis as any).document;
          const links = Array.from(document.querySelectorAll("a"));
          return links.find((link: any) =>
            link.textContent?.includes("Log in")
          );
        });
      }

      if (!loginLink) {
        return true;
      }

      // Check if element is visible
      const isVisible = await loginLink.isVisible();

      if (!isVisible) {
        await loginLink.scrollIntoView();
        await this.delay(1000);
      }

      // Try clicking the login link with error handling
      try {
        await loginLink.click();
      } catch (clickError) {
        await this.page!.evaluate((element) => {
          element.click();
        }, loginLink);
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
        console.log("Already logged in with saved cookies");
        return true;
      }
    }

    // If not logged in, attempt login
    return await this.login();
  }

  async getThreadsNeedingUpdate(): Promise<ForumThread[]> {
    try {
      // Get threads where detailPageUpdateDate is null OR lastReplyDate > detailPageUpdateDate
      const threads = await ForumThread.findAll({
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
        `Found ${threads.length} threads needing detail page updates`
      );
      return threads;
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
      if (cleanUrl.includes("/forums/") || cleanUrl.includes("?prefix_id=")) {
        console.log(`Skipping forum URL: ${cleanUrl}`);
        return;
      }

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

        await this.page!.goto(pageUrl, {
          waitUntil: "networkidle2",
        });

        // Handle cookie consent on first page only
        if (pageNum === 1) {
          await this.handleCookieConsent();
        }

        const posts = await this.scrapePagePosts();

        // Save posts to database after each page
        await this.savePostsToDatabase(thread.threadId, posts);

        // Increment pages scraped counter
        this.pagesScraped++;

        // Check if we need to restart browser
        if (this.pagesScraped >= this.PAGES_BEFORE_RESTART) {
          console.log(
            `Reached ${this.PAGES_BEFORE_RESTART} pages scraped. Restarting browser...`
          );
          await this.restartBrowser();
        }

        // Add delay between pages
        if (pageNum < totalPages) {
          await this.delay(2000);
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

            // Extract media (images and videos) - CORRECTED SELECTORS
            const medias: string[] = [];

            // Get attachments from message-attachments section
            const attachmentSection = element.querySelector(
              ".message-attachments"
            );
            if (attachmentSection) {
              // Get attachment images
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
                  medias.push(src);
                }
              });

              // Get attachment links
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

  // Update savePostsToDatabase method to upload media to S3
  private async savePostsToDatabase(
    threadId: string,
    posts: PostData[]
  ): Promise<void> {
    try {
      // Get all existing posts for this thread to avoid duplicate processing
      const existingPosts = await ForumPost.findAll({
        where: { threadId: threadId },
        attributes: ["postId", "medias"],
      });

      // Create a map of existing post IDs for quick lookup
      const existingPostIds = new Set(existingPosts.map((post) => post.postId));

      console.log(
        `---------------Found ${existingPostIds.size} existing posts for thread ${threadId}`
      );

      for (const postData of posts) {
        // Check if post already exists
        if (existingPostIds.has(postData.postId)) {
          continue;
        }

        // Upload media to S3 and get new URLs
        let processedMedias: string[] = [];

        if (postData.medias.length > 0) {
          try {
            processedMedias = await this.s3Service.uploadMediaUrls(
              postData.medias,
              threadId,
              postData.postId
            );
            if (processedMedias.length > 0) {
              console.log(
                `Successfully uploaded media for post ${postData.postId}, processedMedias: ${processedMedias}`
              );
            }
          } catch (error) {
            console.error(
              `Error uploading media for post ${postData.postId}:`,
              error
            );
          }
        }

        if (processedMedias.length > 0) {
          await ForumPost.upsert({
            postId: postData.postId,
            threadId: threadId,
            author: postData.author,
            content: postData.content,
            medias: JSON.stringify(processedMedias),
          });
          console.log(
            `Successfully saved post ${postData.postId} to database, processedMedias length: ${processedMedias.length}`
          );
        }
      }
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
    if (this.browser) {
      await this.browser.close();
      console.log("Detail page scraper closed");
    }
  }

  async run(): Promise<void> {
    try {
      await this.initialize();

      const threadsToUpdate = await this.getThreadsNeedingUpdate();

      console.log(`Processing ${threadsToUpdate.length} threads`);

      for (const thread of threadsToUpdate) {
        await this.scrapeThreadDetailPage(thread);

        // Add delay between threads
        await this.delay(3000);
      }

      console.log("Detail page scraping completed");
    } catch (error) {
      console.error("Error in detail page scraping:", error);
    } finally {
      await this.close();
    }
  }
}

export { ForumDetailPageScraper };
