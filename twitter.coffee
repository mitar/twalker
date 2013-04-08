limiter = require 'limiter'
twitter = require 'ntwitter'
util = require 'util'

_ = require 'underscore'

twit = new twitter
  consumer_key: process.env.TWITTER_CONSUMER_KEY
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET
  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET

class TwitterRequest
  constructor: (@href, @name, @rateLimit, @toParams, @toResult) ->
    @limiter = new limiter.RateLimiter @rateLimit, 16 * 60 * 1000 # limit of requests per 15 minutes, in ms, but we are using 16 to make sure

    @queue = []

    # This two methods have to be defined in the constructor so that they can access this

    @queueSize = _.throttle =>
        console.warn "#{ @name } queue size is #{ @queue.length } elements"
      , 60 * 1000 # Warn only once per 60 s

    @purgeWarning = _.throttle (requests) =>
        console.warn "#{ @name } rate limit hit, purging #{ requests } requests, #{ @queue.length } in the queue"
      , 10 * 1000 # Warn only once per 10 s

  processQueue: =>
    f = @queue.pop()
    if !f
      return

    @queueSize()

    @limiter.removeTokens 1, (err, remainingRequests) =>
      f()

      @queueSize()

  get: (args..., cb) =>
    page = (cursor, cb) =>
      console.log "Starting #{ @name}: #{ args }, #{ cursor }"

      params = @toParams args...
      params.cursor = cursor

      twit.get @href, params, (err, data) =>
        if err
          err.name = @name
          cb err
          return

        result = @toResult data

        if !data.next_cursor_str or data.next_cursor_str == '0'
          console.log "Finisehd #{ @name }: #{ args }"

          cb null, result
          return

        @queue.push =>
          page data.next_cursor_str, (err, nextResult) =>
            if err
              cb err
              return

            result = result.concat nextResult

            console.log "Finisehd #{ @name }: #{ args }"

            cb null, result

        @processQueue()

    page -1, cb

  fun: (args..., cb) =>
    f = =>
      @get args..., (err, ids) =>
        if err && err.statusCode == 429
          # We have to purge requests, we hit rate limit
          # We purge half each time (to allow faster recovery)
          requests = parseInt(@limiter.tokenBucket.content / 2) || 1
          @purgeWarning requests
          @limiter.removeTokens requests, (err, remainingRequests) =>
            # We retry
            @queue.push f
            @processQueue()

          return

        @processQueue()

        cb err, ids

    @queue.unshift f
    @processQueue()

getFollowers = new TwitterRequest '/followers/ids.json', 'getFollowers', 15, (user_id) ->
  user_id: user_id
  stringify_ids: true
, (data) ->
  data.ids

exports.getFollowers = getFollowers.fun

getFriends = new TwitterRequest '/friends/ids.json', 'getFriends', 15, (user_id) ->
  user_id: user_id
  stringify_ids: true
, (data) ->
  data.ids

exports.getFriends = getFriends.fun

getUsers = new TwitterRequest '/users/lookup.json', 'getUsers', 180, (user_ids...) ->
  user_id: user_ids.join ','
  include_entities: true
, (data) ->
  data

exports.getUsers = getUsers.fun
