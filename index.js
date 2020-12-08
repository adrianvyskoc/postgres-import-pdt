

const fs = require('fs')
const gunzip = require('gunzip-file')
const { Client } = require('pg')
const lineByLine = require('n-readlines')

const QueryFacade = require('./query.facade')
const Constants = require('./constants')

require('dotenv').config()

const client = new Client({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE
})
const args = process.argv.slice(2).reduce((acc, next) => {
  const [key, value] = next.split(':')
  acc[key] = value
  return acc
}, {})

;(async () => {
  try {
    await client.connect()
    await main()
    await client.end()
  } catch(err) {
    console.error(err)
  }
})()

async function main() {
  if (args.unzip)
    await unzipTweetData()
  await importTweetData()

  // ------------------------------------

  async function unzipTweetData() {
    // #0 is .DS_STORE, we can ignore it
    const files = fs.readdirSync(process.env.DATA_SOURCE_FOLDER).slice(1)

    const gunzipPromises = files.map(file => {
      return new Promise((resolve, reject) => {
        const [targetFileName, _] = file.split('.gz')
        gunzip(
          `${process.env.DATA_SOURCE_FOLDER}/${file}`, 
          `${process.env.UNZIPPED_DATA_DESTINATION}/${targetFileName}`, 
          () => {
            console.log(`${targetFileName} was created.`)
            resolve()
          }
        )
      })
    })

    await Promise.all(gunzipPromises)
  }
  
  async function importTweetData() {
    // #0 is .DS_STORE, we can ignore it
    const files = fs.readdirSync(process.env.UNZIPPED_DATA_DESTINATION).slice(1)

    for (const file of files) {
      console.log(`${file} completed.`)
      await processFile(`${process.env.UNZIPPED_DATA_DESTINATION}/${file}`)
    }
  }

  async function processFile(filePath) {
    const liner = new lineByLine(filePath)
    let line
    
    while (line = liner.next()) {      
      const parsedLine = JSON.parse(line)

      try {
        const account = Constants.constructAccount(parsedLine)
        await QueryFacade.insertAccount(account)

        // there is import for tweet_mentions, countries and hashtags made in this method
        const tweetAndHashtagIdsAndRetweet = await Constants.constructTweet(parsedLine)

        if (tweetAndHashtagIdsAndRetweet.original_tweet) {
          const account_rt = Constants.constructAccount(parsedLine.retweeted_status)
          await QueryFacade.insertAccount(account_rt)
          await QueryFacade.insertTweet(tweetAndHashtagIdsAndRetweet.original_tweet.tweet)
        }
        await QueryFacade.insertTweet(tweetAndHashtagIdsAndRetweet.tweet)

        await QueryFacade.insertTweetHashtags(tweetAndHashtagIdsAndRetweet.tweet.id, tweetAndHashtagIdsAndRetweet.hashtag_ids)
        await QueryFacade.insertTweetMentions(tweetAndHashtagIdsAndRetweet.tweet.id, tweetAndHashtagIdsAndRetweet.account_ids)
      } catch (err) {
        console.error(err.stack)
      }
    }

    console.log('IMPORT COMPLETED')
  }
}

