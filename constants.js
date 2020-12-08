var _ = require('lodash')

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

const QueryFacade = require('./query.facade')

module.exports = {

  constructAccount: parsedLine => {
    return {
      'id': parsedLine.user.id,
      'screen_name': parsedLine.user.screen_name,
      'name': parsedLine.user.name,
      'description': parsedLine.user.description,
      'followers_count': parsedLine.user.followers_count,
      'friends_count': parsedLine.user.friends_count,
      'statuses_count': parsedLine.user.statuses_count
    }
  },

  constructTweet: _constructTweet
}

async function _constructTweet(parsedLine) {
  let original_tweet = null
  let hashtag_ids = []
  let account_ids = []
  let tweet = {}
  
  tweet.id = parsedLine.id_str
  tweet.retweet_count = parsedLine.retweet_count
  tweet.favorite_count = parsedLine.favorite_count
  tweet.content = parsedLine.full_text
  tweet.happened_at = parsedLine.created_at
  tweet.author_id = parsedLine.user.id
  
  if (parsedLine.coordinates) {
    tweet.location = parsedLine.coordinates.coordinates
  }
  else 
    tweet.location = null

  if (parsedLine.place) {
    const country = {
      code: parsedLine.place.country_code,
      name: parsedLine.place.country
    }
    const country_id = await QueryFacade.insertCountry(country)
    tweet.country_id = country_id
  } else {
    tweet.country_id = null
  }

  let id = null
  if (parsedLine.entities.hashtags && parsedLine.entities.hashtags.length) {
    for (let hashtag of parsedLine.entities.hashtags) {
      id = await knex.raw(
        'INSERT INTO hashtags (value) VALUES (?) ON CONFLICT (value) DO UPDATE SET value = EXCLUDED.value RETURNING id', 
        [hashtag.text]
      )

      hashtag_ids.push(id.rows[0].id)
    }
  }

  if (parsedLine.retweeted_status) {
    tweet.parent_id = parsedLine.retweeted_status.id_str
    original_tweet = await _constructTweet(parsedLine.retweeted_status)
  } else {
    tweet.parent_id = null
  }

  if (parsedLine.entities.user_mentions && parsedLine.entities.user_mentions.length) {
    for (let user of parsedLine.entities.user_mentions) {
      const id = await knex.raw(
        'INSERT INTO accounts (screen_name, name, id) VALUES (?,?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id', 
        [user.screen_name, user.name, user.id]
      )
      account_ids.push(id.rows[0].id)
    }
  } 
  return {
    tweet, hashtag_ids, original_tweet, account_ids
  }
}