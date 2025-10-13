import puppeteer, { Browser, Page } from "puppeteer";
import * as fs from "fs";
import * as path from "path";
import dotenv from "dotenv";
import { ForumThread } from "../model/ForumThread";
dotenv.config();

interface ThreadUpdateData {
  threadId: number;
  replies: string;
  views: string;
  lastReplyDate: string;
  lastReplier: string;
}

interface LoginCredentials {
  username: string;
  password: string;
}

class ForumThreadLatestUpdateChecker {
  private browser: Browser | null = null;
  private page: Page | null = null;
  private cookiesPath: string;
  private credentials: LoginCredentials;
  private mode: string;

  // Site URLs
  private readonly SITE_URL = "https://www.lpsg.com";
  private readonly FORUM_URL = `${this.SITE_URL}/forums/models-and-celebrities.17/`;
  private readonly LOGIN_URL = `${this.SITE_URL}/login/`;

  constructor() {
    this.cookiesPath = path.join(__dirname, "../../cookies.json");
    this.credentials = {
      username: process.env.FORUM_USERNAME || "",
      password: process.env.FORUM_PASSWORD || "",
    };
    this.mode = process.env.NODE_ENV || "";
  }

  async initialize(): Promise<void> {
    console.log("Initializing browser...");
    this.browser = await puppeteer.launch({
      headless: this.mode === "production" ? true : true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
      defaultViewport: { width: 1280, height: 720 },
    });

    this.page = await this.browser.newPage();

    await this.page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    );
  }

  async loadCookies(): Promise<boolean> {
    try {
      if (fs.existsSync(this.cookiesPath)) {
        const cookies = JSON.parse(fs.readFileSync(this.cookiesPath, "utf8"));
        await this.page!.setCookie(...cookies);
        console.log("Cookies loaded successfully");
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
      console.log("Cookies saved successfully");
    } catch (error) {
      console.error("Error saving cookies:", error);
    }
  }

  private async handleCookieConsent(): Promise<void> {
    try {
      const cookieButton = await this.page!.$(
        'a[href*="/misc/cookies"][class*="button--notice"]'
      );

      if (cookieButton) {
        await cookieButton.click();
        await this.delay(1000);
      }
    } catch (error) {}
  }

  async checkLoginStatus(): Promise<boolean> {
    try {
      await this.page!.goto(this.FORUM_URL, {
        waitUntil: "networkidle2",
      });

      await this.handleCookieConsent();

      const userAccountLink = await this.page!.$('a[href="/account/"]');
      return !!userAccountLink;
    } catch (error) {
      console.error("Error checking login status:", error);
      return false;
    }
  }

  async login(): Promise<boolean> {
    try {
      console.log("Attempting to login...");

      await this.page!.goto(this.LOGIN_URL, {
        waitUntil: "networkidle2",
      });

      await this.handleCookieConsent();
      await this.delay(2000);

      let loginElement = await this.page!.$('a[href="/login/"]');

      if (!loginElement) {
        loginElement = await this.page!.$("a.p-navgroup-link--logIn");
      }

      if (!loginElement) {
        console.log("Login link not found - might already be logged in");
        return true;
      }

      await loginElement.click();
      console.log("Clicked login link, waiting for modal to open...");
      await this.delay(2000);

      let loginForm = await this.page!.$('input[name="login"]');

      if (!loginForm) {
        console.log(
          "Still no login form found, taking screenshot for debugging..."
        );
        await this.page!.screenshot({ path: "debug-login.png" });
        console.log("Screenshot saved as debug-login.png");
        return false;
      }

      console.log("Login form found, proceeding with login...");
      await this.delay(1000);

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

      let submitButton = await this.page!.$(
        'button[type="submit"].button--primary.button--icon--login'
      );

      if (submitButton) {
        await submitButton.click();
        console.log("Submitted login form...");
      } else {
        console.log("Submit button not found");
        return false;
      }

      await this.delay(3000);

      const userAccountLink = await this.page!.$('a[href="/account/"]');

      if (userAccountLink) {
        console.log("Login successful!");
        await this.saveCookies();
        return true;
      } else {
        console.log("Login failed - user account link not found");
        return false;
      }
    } catch (error) {
      console.error("Error during login:", error);
      return false;
    }
  }

  async ensureLoggedIn(): Promise<boolean> {
    const cookiesLoaded = await this.loadCookies();

    if (cookiesLoaded) {
      const isLoggedIn = await this.checkLoginStatus();
      if (isLoggedIn) {
        console.log("Already logged in with saved cookies");
        return true;
      }
    }

    return await this.login();
  }

  private async getLastPageNumber(): Promise<number> {
    try {
      const lastPageNumber = await this.page!.evaluate(() => {
        const document = (globalThis as any).document;
        const pageNav = document.querySelector(".pageNav-main");
        if (!pageNav) return 1;

        const lastPageLink = pageNav.querySelector("li:last-child a");
        if (!lastPageLink) return 1;

        const href = lastPageLink.getAttribute("href");
        const match = href.match(/page-(\d+)$/);
        return match ? parseInt(match[1]) : 1;
      });

      console.log(`Detected last page: ${lastPageNumber}`);
      return lastPageNumber;
    } catch (error) {
      console.error("Error getting last page number:", error);
      return 1;
    }
  }

  /**
   * Check for updates on forum listing pages
   * Returns true if at least one thread has the same lastReplyDate as in DB (no updates needed)
   */
  async checkForUpdates(): Promise<boolean> {
    try {
      console.log("Checking for thread updates...");

      // Get all threads from database
      const dbThreads = await ForumThread.findAll({
        order: [["lastReplyDate", "DESC"]],
      });

      if (dbThreads.length === 0) {
        console.log("No threads in database to check");
        return false;
      }

      console.log(`Checking ${dbThreads.length} threads from database`);

      // Create a map of threadId -> lastReplyDate from DB
      const dbThreadMap = new Map<number, string>();
      dbThreads.forEach((thread) => {
        dbThreadMap.set(thread.threadId, thread.lastReplyDate);
      });

      // Get the last page number
      const lastPageNumber = await this.getLastPageNumber();
      let foundMatchingThread = false;
      let updatedThreads: ThreadUpdateData[] = [];

      // Loop through all pages to check for updates
      for (let pageNum = 1; pageNum <= lastPageNumber; pageNum++) {
        try {
          console.log(`Checking page ${pageNum} of ${lastPageNumber}...`);

          const pageUrl =
            pageNum === 1 ? this.FORUM_URL : `${this.FORUM_URL}page-${pageNum}`;

          let navigationSuccess = false;
          for (let retry = 0; retry < 3; retry++) {
            try {
              await this.page!.goto(pageUrl, {
                waitUntil: "networkidle2",
                timeout: 30000,
              });
              navigationSuccess = true;
              break;
            } catch (navError) {
              console.log(
                `Navigation attempt ${retry + 1} failed for page ${pageNum}:`,
                navError
              );
              if (retry < 2) {
                await this.delay(3000);
              }
            }
          }

          if (!navigationSuccess) {
            console.error(
              `Failed to navigate to page ${pageNum} after 3 attempts, skipping...`
            );
            continue;
          }

          if (pageNum === 1) {
            await this.handleCookieConsent();
          }

          // Wait for threads to load
          let threadsLoaded = false;
          const selectors = [
            ".structItem",
            ".structItem-container",
            ".thread-list",
          ];

          for (const selector of selectors) {
            try {
              await this.page!.waitForSelector(selector, { timeout: 15000 });
              threadsLoaded = true;
              break;
            } catch (selectorError) {
              console.log(`Selector ${selector} not found, trying next...`);
            }
          }

          if (!threadsLoaded) {
            console.error(`No thread selectors found on page ${pageNum}`);
            continue;
          }

          const threads = await this.page!.evaluate(() => {
            const document = (globalThis as any).document;
            const threads: any[] = [];

            // Get ONLY the normal threads container (NOT the sticky one)
            // Sticky threads are in: .structItemContainer-group--sticky
            // Normal threads are in: .structItemContainer-group (without --sticky)
            const normalThreadsContainer = document.querySelector(
              ".structItemContainer-group:not(.structItemContainer-group--sticky)"
            );

            if (!normalThreadsContainer) {
              console.log("Normal threads container not found");
              return [];
            }

            // Get thread elements from the normal container only
            const threadElements =
              normalThreadsContainer.querySelectorAll(".structItem");

            threadElements.forEach((element: any) => {
              try {
                const threadIdStr =
                  element.getAttribute("data-thread-id") ||
                  element.className.match(/js-threadListItem-(\d+)/)?.[1] ||
                  "";

                const threadId = parseInt(threadIdStr) || 0;

                const repliesElement = element.querySelector(
                  ".structItem-cell--meta dl:nth-child(1) dd"
                );
                const replies = repliesElement?.textContent?.trim() || "0";

                const viewsElement = element.querySelector(
                  ".structItem-cell--meta dl:nth-child(2) dd"
                );
                const views = viewsElement?.textContent?.trim() || "0";

                const lastReplyElement = element.querySelector(
                  ".structItem-cell--latest time"
                );
                const lastReplyDate =
                  lastReplyElement?.getAttribute("datetime") || "";

                const lastReplierElement = element.querySelector(
                  ".structItem-cell--latest .username"
                );
                const lastReplier =
                  lastReplierElement?.textContent?.trim() || "";

                if (threadId > 0 && lastReplyDate) {
                  threads.push({
                    threadId,
                    replies,
                    views,
                    lastReplyDate,
                    lastReplier,
                  });
                }
              } catch (error) {
                console.error("Error parsing thread element:", error);
              }
            });

            return threads;
          });

          console.log(`Found ${threads.length} threads on page ${pageNum}`);

          // Check each thread against the database
          for (const thread of threads) {
            const dbLastReplyDate = dbThreadMap.get(thread.threadId);

            if (dbLastReplyDate) {
              // Thread exists in DB, check if lastReplyDate matches
              if (dbLastReplyDate === thread.lastReplyDate) {
                console.log(
                  `✓ Thread ${thread.threadId} has same lastReplyDate as DB: ${thread.lastReplyDate}`
                );
                foundMatchingThread = true;
                break; // Stop checking this page
              } else {
                console.log(
                  `✗ Thread ${thread.threadId} has different lastReplyDate - DB: ${dbLastReplyDate}, Current: ${thread.lastReplyDate}`
                );
                updatedThreads.push({
                  threadId: thread.threadId,
                  replies: thread.replies,
                  views: thread.views,
                  lastReplyDate: thread.lastReplyDate,
                  lastReplier: thread.lastReplier,
                });
              }
            }
          }

          // If we found at least one matching thread, stop scraping
          if (foundMatchingThread) {
            console.log(
              "Found at least one thread with matching lastReplyDate. Stopping scraping."
            );
            break;
          }

          // Add delay between pages
          if (pageNum < lastPageNumber) {
            await this.delay(2000);
          }
        } catch (pageError) {
          console.error(`Error checking page ${pageNum}:`, pageError);
          await this.delay(3000);
        }
      }

      // Update threads in database if there are changes
      if (updatedThreads.length > 0) {
        console.log(`Updating ${updatedThreads.length} threads in database...`);
        await this.updateThreadsInDatabase(updatedThreads);
        console.log("Database update completed");
      }

      return true;
    } catch (error) {
      console.error("Error checking for updates:", error);
      return false;
    }
  }

  private async updateThreadsInDatabase(
    threads: ThreadUpdateData[]
  ): Promise<void> {
    try {
      const BATCH_SIZE = 50;
      const totalBatches = Math.ceil(threads.length / BATCH_SIZE);

      for (let i = 0; i < threads.length; i += BATCH_SIZE) {
        const batch = threads.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;

        console.log(
          `Processing batch ${batchNum}/${totalBatches} (${batch.length} threads)`
        );

        // Process all updates in this batch in parallel
        const updatePromises = batch.map((threadData) =>
          ForumThread.update(
            {
              replies: threadData.replies,
              views: threadData.views,
              lastReplyDate: threadData.lastReplyDate,
              lastReplier: threadData.lastReplier,
            },
            {
              where: { threadId: threadData.threadId },
            }
          ).then(() => {
            console.log(
              `Updated thread ${threadData.threadId} - Replies: ${threadData.replies}, Views: ${threadData.views}`
            );
          })
        );

        await Promise.all(updatePromises);
        console.log(`✓ Completed batch ${batchNum}/${totalBatches}`);
      }

      console.log(`✓ All ${threads.length} threads updated successfully`);
    } catch (error) {
      console.error("Error updating threads in database:", error);
    }
  }

  async close(): Promise<void> {
    if (this.browser) {
      await this.browser.close();
      console.log("Browser closed");
    }
  }

  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async run(): Promise<void> {
    try {
      await this.initialize();

      const isLoggedIn = await this.ensureLoggedIn();
      if (!isLoggedIn) {
        throw new Error("Failed to login");
      }

      const foundMatch = await this.checkForUpdates();
    } catch (error) {
      console.error("Error in main execution:", error);
    } finally {
      await this.close();
    }
  }
}

export { ForumThreadLatestUpdateChecker };

// Run the checker if this file is executed directly
if (require.main === module) {
  const checker = new ForumThreadLatestUpdateChecker();
  checker.run().catch(console.error);
}
