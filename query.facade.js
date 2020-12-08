const knexPostgis = require('knex-postgis')
const knex = require('knex')({
  client: 'pg',
  connection: {
    host: 'localhost',
    user: 'adrianvyskoc',
    password: '12345',
    database: 'pdt'
  }
})
const st = knexPostgis(knex)

class QueryFacade {
  async insertAccount(account) {
    await knex.raw(
      `
        INSERT INTO accounts (description, followers_count, friends_count, id, name, screen_name, statuses_count) 
        VALUES (?,?,?,?,?,?,?) 
        ON CONFLICT (id) DO UPDATE
        SET 
        description = EXCLUDED.description,
        followers_count = EXCLUDED.followers_count,
        friends_count = EXCLUDED.friends_count,
        statuses_count = EXCLUDED.statuses_count
      `, 
      [
        account.description, 
        account.followers_count, 
        account.friends_count, 
        account.id, 
        account.name, 
        account.screen_name, 
        account.statuses_count
      ]
    )
  }

  async insertCountry(country) {
    const country_id = await knex.raw(
      'INSERT INTO countries (code, name) VALUES (?,?) ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code  RETURNING id', 
      [country.code, country.name]
    )
    return country_id.rows[0].id
  }

  async insertTweet(tweet) {
    await knex.raw(
      `INSERT INTO tweets 
      (
        id,
        retweet_count,
        favorite_count,
        content,
        happened_at,
        author_id,
        country_id,
        parent_id,
        location
      ) 
      VALUES (?,?,?,?,?,?,?,?, ST_SetSRID(ST_Point(?, ?), 4326)) ON CONFLICT (id) DO NOTHING
      `,
      [
        tweet.id,
        tweet.retweet_count,
        tweet.favorite_count,
        tweet.content,
        tweet.happened_at,
        tweet.author_id ? tweet.author_id : null,
        tweet.country_id ? tweet.country_id : null,
        tweet.parent_id ? tweet.parent_id : null,
        tweet.location && tweet.location[0], // lon
        tweet.location && tweet.location[1]  // lat
      ]
    )
  }

  async insertTweetHashtags(tweet_id, hashtag_ids) {
    hashtag_ids.forEach(async hashtag_id => {
      await knex('tweet_hashtags').insert({ hashtag_id, tweet_id })
    })
  }

  async insertTweetMentions(tweet_id, account_ids) {
    account_ids.forEach(async account_id => {
      await knex('tweet_mentions').insert({ account_id, tweet_id })
    })
  }
}

module.exports = new QueryFacade()
