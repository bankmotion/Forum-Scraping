import 'reflect-metadata';
import { sequelize } from './src/config/database';
import { ForumScraper } from './src/service/forumScrapingScript';
import { ForumDetailPageScraper } from './src/service/forumDetailPageScraping';

async function main() {
  try {
    // Test connection
    await sequelize.authenticate();
    console.log('Database connection established successfully.');
    
    // Sync database (create tables if they don't exist)
    await sequelize.sync();
    console.log('Database synchronized successfully.');

    // Run the forum scraper
    // console.log('Starting forum scraping...');
    // const scraper = new ForumScraper();
    // await scraper.run();


    console.log('Starting detail page scraping...');
    const detailPageScraper = new ForumDetailPageScraper();
    await detailPageScraper.run();
  } catch (error) {
    console.error('Error in main execution:', error);
  }
}

main();
