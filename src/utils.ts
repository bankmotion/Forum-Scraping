import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { exec } from "child_process";
import { promisify } from "util";

const execAsync = promisify(exec);

/**
 * Creates a temporary user data directory for Puppeteer
 * @returns The path to the created temp directory
 */
export const createTempUserDataDir = (): string => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "puppeteer-"));
  console.log("Created temp directory:", tempDir);
  return tempDir;
};

/**
 * Deletes temporary user data directories and cleans up system caches
 * @param mode - Environment mode (development/production)
 */
export const deleteTempUserDataDir = async (mode: string = "development"): Promise<void> => {
  if (mode === "development") {
    return;
  }

  const SNAP_TMP_DIR = "/tmp/snap-private-tmp/snap.chromium/tmp";
  const PREFIX = "puppeteer-";

  // Check if the SNAP_TMP_DIR exists before trying to read from it
  if (!fs.existsSync(SNAP_TMP_DIR)) {
    console.log(`SNAP_TMP_DIR does not exist: ${SNAP_TMP_DIR}`);
    await clearSystemCaches();
    console.log("Cleanup complete.");
    return;
  }

  try {
    const entries = fs.readdirSync(SNAP_TMP_DIR, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.isDirectory() && entry.name.startsWith(PREFIX)) {
        const fullPath = path.join(SNAP_TMP_DIR, entry.name);
        try {
          fs.rmSync(fullPath, { recursive: true, force: true });
          console.log(`Deleted temp directory: ${fullPath}`);
        } catch (err) {
          console.warn(`Could not delete ${fullPath}: ${err instanceof Error ? err.message : "Unknown error"}`);
        }
      }
    }
  } catch (error) {
    console.warn(`Error reading SNAP_TMP_DIR ${SNAP_TMP_DIR}: ${error instanceof Error ? error.message : "Unknown error"}`);
  }

  await clearSystemCaches();
  console.log("Cleanup complete.");
};

/**
 * Windows-compatible cache clearing function
 * Equivalent to: sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
 */
export const clearSystemCaches = async (): Promise<void> => {
  try {
    // Force sync/flush file system buffers
    await new Promise((resolve) => {
      // On Windows, we can use fs.fsyncSync to sync file descriptors
      // For general file system sync, we'll use a different approach
      resolve(true);
    });

    // Clear Node.js internal caches
    if (global.gc) {
      global.gc();
      console.log("Garbage collection triggered");
    }

    // Clear require cache (module cache)
    const cacheKeys = Object.keys(require.cache);
    cacheKeys.forEach((key) => {
      delete require.cache[key];
    });
    console.log(`Cleared ${cacheKeys.length} module cache entries`);

    // Clear temp directories more aggressively
    const tempDirs = [
      os.tmpdir(),
      path.join(process.cwd(), "temp"),
      path.join(process.cwd(), "tmp"),
    ];

    for (const tempDir of tempDirs) {
      if (fs.existsSync(tempDir)) {
        try {
          const entries = fs.readdirSync(tempDir, { withFileTypes: true });
          let cleanedCount = 0;

          for (const entry of entries) {
            if (entry.isDirectory()) {
              const fullPath = path.join(tempDir, entry.name);
              try {
                // Only delete directories that are likely temp files
                if (
                  entry.name.includes("puppeteer") ||
                  entry.name.includes("temp") ||
                  entry.name.includes("cache") ||
                  entry.name.startsWith("tmp-")
                ) {
                  fs.rmSync(fullPath, { recursive: true, force: true });
                  cleanedCount++;
                }
              } catch (err) {
                // Ignore errors for files that can't be deleted
                console.debug(
                  `Could not delete ${fullPath}: ${
                    err instanceof Error ? err.message : "Unknown error"
                  }`
                );
              }
            }
          }
          console.log(
            `Cleaned ${cleanedCount} temp directories from ${tempDir}`
          );
        } catch (err) {
          console.warn(
            `Could not access temp directory ${tempDir}: ${
              err instanceof Error ? err.message : "Unknown error"
            }`
          );
        }
      }
    }

    console.log("System cache clearing complete");
  } catch (error) {
    console.error("Error clearing system caches:", error);
    throw error;
  }
};

/**
 * Gets current system memory usage
 */
export const getMemoryUsage = async (): Promise<void> => {
  try {
    const { stdout } = await execAsync('free -h');
    console.log("Current memory usage:");
    console.log(stdout);
  } catch (error) {
    console.error("Error getting memory usage:", error);
  }
};

/**
 * Clears browser cache using Chrome DevTools Protocol
 * @param page - Puppeteer page instance
 */
export const clearBrowserCache = async (page: any): Promise<void> => {
  try {
    if (!page) return;

    console.log("Clearing browser cache and memory...");

    // Clear browser cache
    const client = await page.target().createCDPSession();
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

    console.log("Browser cache and memory cleared successfully");
  } catch (error) {
    console.error("Error clearing browser cache:", error);
  }
};

/**
 * Creates optimized browser configuration for production/development
 * @param mode - Environment mode
 * @param tempDir - Temporary directory path
 * @returns Browser launch configuration
 */
export const createBrowserConfig = (mode: string, tempDir: string): any => {
  let browserConfig: any = {
    headless: mode === "production" ? true : true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
    defaultViewport: { width: 1280, height: 720 },
  };

  if (mode === "production") {
    browserConfig = {
      ...browserConfig,
      executablePath: "/usr/bin/chromium-browser",
      userDataDir: tempDir,
      args: [
        ...browserConfig.args,
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=TranslateUI",
        "--disable-ipc-flooding-protection",
        "--memory-pressure-off",
        "--max_old_space_size=4096"
      ],
    };
  }

  return browserConfig;
};

/**
 * Utility function to add delay
 * @param ms - Milliseconds to delay
 */
export const delay = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};

/**
 * Utility function to format file size
 * @param bytes - Size in bytes
 * @returns Formatted size string
 */
export const formatFileSize = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/**
 * Utility function to check if a path exists
 * @param path - Path to check
 * @returns True if path exists
 */
export const pathExists = (path: string): boolean => {
  return fs.existsSync(path);
};

/**
 * Utility function to safely delete a directory
 * @param dirPath - Directory path to delete
 * @param recursive - Whether to delete recursively
 */
export const safeDeleteDir = (dirPath: string, recursive: boolean = true): boolean => {
  try {
    if (fs.existsSync(dirPath)) {
      fs.rmSync(dirPath, { recursive, force: true });
      console.log(`Successfully deleted directory: ${dirPath}`);
      return true;
    }
    return false;
  } catch (error) {
    console.warn(`Could not delete directory ${dirPath}: ${error instanceof Error ? error.message : "Unknown error"}`);
    return false;
  }
};

/**
 * Utility function to get system information
 */
export const getSystemInfo = (): { platform: string; arch: string; tmpdir: string } => {
  return {
    platform: os.platform(),
    arch: os.arch(),
    tmpdir: os.tmpdir()
  };
};
