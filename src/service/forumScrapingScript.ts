import puppeteer, { Browser, Page } from "puppeteer";
import * as fs from "fs";
import * as path from "path";
import dotenv from "dotenv";
import { ForumThread } from "../model/ForumThread";
dotenv.config();

interface ForumThreadData {
  threadId: string;
  title: string;
  creator: string;
  creationDate: string;
  replies: string;
  views: string;
  lastReplyDate: string;
  lastReplier: string;
  threadUrl: string;
  detailPageUpdateDate?: string | null; // Changed to string
}

interface LoginCredentials {
  username: string;
  password: string;
}

class ForumScraper {
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
      headless: this.mode === "production" ? true : false,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
      defaultViewport: { width: 1280, height: 720 },
    });

    this.page = await this.browser.newPage();

    // Set user agent to avoid detection
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
      // Check if cookie consent button exists
      const cookieButton = await this.page!.$(
        'a[href*="/misc/cookies"][class*="button--notice"]'
      );

      if (cookieButton) {
        console.log(
          "Cookie consent dialog found, clicking Accept all cookies..."
        );
        await cookieButton.click();
        await this.delay(1000); // Wait for dialog to close
        console.log("Cookie consent accepted");
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
      return !!userAccountLink;
    } catch (error) {
      console.error("Error checking login status:", error);
      return false;
    }
  }

  async login(): Promise<boolean> {
    try {
      console.log("Attempting to login...");

      // Navigate to the forum page first
      await this.page!.goto(this.LOGIN_URL, {
        waitUntil: "networkidle2",
      });

      // Handle cookie consent first
      await this.handleCookieConsent();

      // Wait a bit for the page to fully load
      await this.delay(2000);

      // Try multiple selectors for the login link
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
      // Wait a bit for modal to start opening
      await this.delay(2000);

      // Try multiple selectors for the login form
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

      // Alternative approach - try multiple selectors for the submit button
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

      // Wait for the modal to close and page to update
      await this.delay(3000);

      // Check if login was successful by looking for user account link
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

  // Add method to get the last page number
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

  // Update scrapeForumThreads to save to MySQL
  async scrapeForumThreads(): Promise<ForumThreadData[]> {
    try {
      console.log("Scraping forum threads...");

      // First, get the last page number
      const lastPageNumber = await this.getLastPageNumber();
      const allThreads: ForumThreadData[] = [];
      let failedPages: number[] = [];

      // Loop through ALL pages (no limit)
      for (let pageNum = 1; pageNum <= lastPageNumber; pageNum++) {
        try {
          console.log(`Scraping page ${pageNum} of ${lastPageNumber}...`);

          const pageUrl =
            pageNum === 1 ? this.FORUM_URL : `${this.FORUM_URL}page-${pageNum}`;

          // Navigate to page with retry logic
          let navigationSuccess = false;
          for (let retry = 0; retry < 3; retry++) {
            try {
              await this.page!.goto(pageUrl, {
                waitUntil: "networkidle2",
                timeout: 30000, // Increase timeout to 30 seconds
              });
              navigationSuccess = true;
              break;
            } catch (navError) {
              console.log(
                `Navigation attempt ${retry + 1} failed for page ${pageNum}:`,
                navError
              );
              if (retry < 2) {
                await this.delay(3000); // Wait 3 seconds before retry
              }
            }
          }

          if (!navigationSuccess) {
            console.error(
              `Failed to navigate to page ${pageNum} after 3 attempts, skipping...`
            );
            failedPages.push(pageNum);
            continue;
          }

          // Handle cookie consent on first page only
          if (pageNum === 1) {
            await this.handleCookieConsent();
          }

          // Wait for threads to load with multiple fallback strategies
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
              console.log(`Found threads using selector: ${selector}`);
              break;
            } catch (selectorError) {
              console.log(`Selector ${selector} not found, trying next...`);
            }
          }

          if (!threadsLoaded) {
            console.error(
              `No thread selectors found on page ${pageNum}, checking page content...`
            );

            // Check if page loaded correctly by looking for any forum-related content
            const pageContent = await this.page!.evaluate(() => {
              return (globalThis as any).document.body.innerText;
            });

            if (
              pageContent.includes("404") ||
              pageContent.includes("Not Found") ||
              pageContent.includes("Error")
            ) {
              console.log(
                `Page ${pageNum} appears to be an error page, skipping...`
              );
              failedPages.push(pageNum);
              continue;
            }

            // Try to extract threads even without waiting for selector
            console.log(
              `Attempting to extract threads from page ${pageNum} without selector...`
            );
          }

          const threads = await this.page!.evaluate(() => {
            const threadElements = (
              globalThis as any
            ).document.querySelectorAll(".structItem");
            const threads: any[] = [];

            threadElements.forEach((element: any) => {
              try {
                // Extract thread ID from data attribute
                const threadId =
                  element.getAttribute("data-thread-id") ||
                  element.className.match(/js-threadListItem-(\d+)/)?.[1] ||
                  "";

                const titleElement = element.querySelector(
                  ".structItem-title a"
                );
                const title = titleElement?.textContent?.trim() || "";

                // Extract thread URL from the title link
                const threadUrl = titleElement?.getAttribute("href") || "";

                const creatorElement = element.querySelector(
                  ".structItem-minor .username"
                );
                const creator = creatorElement?.textContent?.trim() || "";

                const creationDateElement = element.querySelector(
                  ".structItem-minor time"
                );
                const creationDate =
                  creationDateElement?.getAttribute("datetime") || "";

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

                if (title && threadId) {
                  threads.push({
                    threadId,
                    title,
                    creator,
                    creationDate,
                    replies,
                    views,
                    lastReplyDate,
                    lastReplier,
                    threadUrl, // Add thread URL
                  });
                }
              } catch (error) {
                console.error("Error parsing thread element:", error);
              }
            });

            return threads;
          });

          if (threads.length === 0) {
            console.log(
              `No threads found on page ${pageNum}, might be empty or error page`
            );
            failedPages.push(pageNum);
          } else {
            allThreads.push(...threads);
            console.log(
              `Scraped ${threads.length} threads from page ${pageNum}`
            );

            // Save to MySQL database after each page
            await this.saveThreadsToDatabase(threads);
          }

          // Add delay between pages to avoid being blocked
          if (pageNum < lastPageNumber) {
            await this.delay(2000);
          }
        } catch (pageError) {
          console.error(`Error scraping page ${pageNum}:`, pageError);
          failedPages.push(pageNum);

          // Continue to next page instead of stopping the entire process
          console.log(`Continuing to next page...`);

          // Add delay before continuing
          await this.delay(3000);
        }
      }

      console.log(
        `Total scraped ${allThreads.length} threads from ${lastPageNumber} pages`
      );

      if (failedPages.length > 0) {
        console.log(`Failed pages: ${failedPages.join(", ")}`);
        console.log(`You may want to retry these pages later`);
      }

      return allThreads;
    } catch (error) {
      console.error("Error scraping forum threads:", error);
      return [];
    }
  }

  // Replace saveThreadsToFile with saveThreadsToDatabase
  private async saveThreadsToDatabase(
    threads: ForumThreadData[]
  ): Promise<void> {
    try {
      for (const threadData of threads) {
        await ForumThread.upsert({
          threadId: threadData.threadId,
          title: threadData.title,
          creator: threadData.creator,
          creationDate: threadData.creationDate,
          replies: threadData.replies,
          views: threadData.views,
          lastReplyDate: threadData.lastReplyDate,
          lastReplier: threadData.lastReplier,
          threadUrl: threadData.threadUrl,
        });
      }
      console.log(`Saved ${threads.length} threads to database`);
    } catch (error) {
      console.error("Error saving threads to database:", error);
    }
  }

  async scrapeThreadContent(threadUrl: string): Promise<any> {
    try {
      console.log(`Scraping thread content: ${threadUrl}`);

      await this.page!.goto(threadUrl, {
        waitUntil: "networkidle2",
      });

      // Wait for posts to load
      await this.page!.waitForSelector(".message", { timeout: 10000 });

      const posts = await this.page!.evaluate(() => {
        const postElements = (globalThis as any).document.querySelectorAll(
          ".message"
        );
        const posts: any[] = [];

        postElements.forEach((element: any) => {
          try {
            const authorElement = element.querySelector(
              ".message-userDetails .username"
            );
            const author = authorElement?.textContent?.trim() || "";

            const dateElement = element.querySelector(
              ".message-userDetails time"
            );
            const date = dateElement?.getAttribute("datetime") || "";

            const contentElement = element.querySelector(
              ".message-content .bbWrapper"
            );
            const content = contentElement?.textContent?.trim() || "";

            const postNumberElement = element.querySelector(
              ".message-attribution-opposite a"
            );
            const postNumber = postNumberElement?.textContent?.trim() || "";

            if (author && content) {
              posts.push({
                author,
                date,
                content,
                postNumber,
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
      console.error("Error scraping thread content:", error);
      return [];
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

      const threads = await this.scrapeForumThreads();

      // Display summary of scraped threads
      console.log(`\nScraping completed! Total threads: ${threads.length}`);
    } catch (error) {
      console.error("Error in main execution:", error);
    } finally {
      await this.close();
    }
  }
}

// Export the class for use in other files
export { ForumScraper };

// Run the scraper if this file is executed directly
if (require.main === module) {
  const scraper = new ForumScraper();
  scraper.run().catch(console.error);
}
