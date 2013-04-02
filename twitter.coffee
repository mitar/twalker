limiter = require 'limiter'
twitter = require 'ntwitter'
util = require 'util'

_ = require 'underscore'

twit = new twitter
  consumer_key: process.env.TWITTER_CONSUMER_KEY
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET
  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET

getFollowersLimiter = new limiter.RateLimiter 15, 15 * 60 * 1000 # 15 requests per 15 minutes, in ms

getFollowersQueue = []

getFollowersQueueSize = _.throttle ->
  console.warn "getFollowers queue size is %s elements", getFollowersQueue.length
, 60 * 1000 # Warn only once per 60 s

processFollowersQueue = ->
  f = getFollowersQueue.pop()
  if !f
    return

  getFollowersQueueSize()

  getFollowersLimiter.removeTokens 1, (err, remainingRequests) ->
    f()

    getFollowersQueueSize()

getFollowers = (user_id, cb) ->
  page = (cursor, cb) ->
    params =
      user_id: user_id
      stringify_ids: true
      cursor: cursor

    console.log params
    twit.get '/followers/ids.json', params, (err, data) ->
      if err
        cb err
        return

      if data.next_cursor_str == '0'
        cb null, data.ids
        return

      getFollowersQueue.push ->
        page data.next_cursor_str, (err, nextIds) ->
          if err
            cb err
            return

          data.ids.push nextIds...
          cb null, data.ids

      processFollowersQueue()

  page -1, cb

exports.getFollowers = (user_id, cb) ->
  f = ->
    getFollowers user_id, (err, ids) ->
      if err && err.statusCode == 429
        # We have to purge requests, we hit rate limit
        # We purge half each time (to allow faster recovery)
        requests = parseInt(getFollowersLimiter.tokenBucket.content / 2) || 1
        getFollowersLimiter.removeTokens requests, (err, remainingRequests) ->
          # We retry
          getFollowersQueue.unshift f
          processFollowersQueue()

        return

      processFollowersQueue()

      cb err, ids

  getFollowersQueue.unshift f
  processFollowersQueue()
