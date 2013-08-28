async = require 'async'
assert = require 'assert'

_ = require 'underscore'

location = require './location'
models = require './models'
twitter = require './twitter'

markInNetwork = (cb) ->
  count = 0

  models.User.find(
    in_network: null
    deleted: {$ne: true}
    has_data: true
  ).batchSize(10).stream().on('error', (err) ->
    console.error "markInNetwork 1 error: #{ err }"
    cb null, 0
    return
  ).on('data', (user) ->
    in_network = user.data.time_zone == 'Ljubljana' or location.REGEX.test(user.data.location) or false
    models.User.findOneAndUpdate
      twitter_id: user.twitter_id
    ,
      in_network: in_network
    , (err) ->
      if err
        console.error "markInNetwork 2 error: #{ user.twitter_id }: #{ err }"
      else
        count++ if in_network
  ).on('close', ->
    cb null, count
  )

addFollowersAndFriends = (cb) ->
  ids = []

  console.log "Processing followers"
  models.User.find(
    has_followers: true
  ).batchSize(10).stream().on('error', (err) ->
    console.error "addFollowersAndFriends 1 error: #{ err }"
    cb null, count
    return
  ).on('data', (user) ->
    for follower in user.followers
      ids.push follower
  ).on('close', ->
    console.log "Processing friends"
    models.User.find(
      has_friends: true
    ).batchSize(10).stream().on('error', (err) ->
      console.error "addFollowersAndFriends 2 error: #{ err }"
      cb null, count
      return
    ).on('data', (user) ->
      for friend in user.friends
        ids.push friend
    ).on('close', ->
      console.log "Storing"
      async.forEachSeries ids, (id, cb) ->
        models.User.update
          twitter_id: id
        ,
          $set: {twitter_id: id}
        ,
          upsert: true
        , (err, numberAffected, rawResponse) ->
          assert.equal numberAffected, 1 if not err
          console.error "addFollowersAndFriends 3 error: #{ id }: #{ err }" if err
          console.log id
          cb null

      , (err) ->
        cb null, ids.length
    )
  )

findFriends = (cb) ->
  models.User.find
    has_friends: {$ne: true}
    in_network: true
    deleted: {$ne: true}
    private: {$ne: true}
  ,
    null
  ,
    limit: 1000
    batchSize: 100
  , (err, users) ->
    if (err)
      console.error "findFriends 1 error: #{ err }"
      cb null, 0
      return

    count = 0
    users = _.shuffle users
    users = users[0...100]

    async.forEachSeries users, (user, cb) ->
      twitter.getFriends user.twitter_id, (err, friends) ->
        if err
          console.error "findFriends 2 error: #{ user.twitter_id }: #{ err }"
          if err.statusCode == 401
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {private: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "findFriends 3 error: #{ user.twitter_id }: #{ err }" if err
          else if err.statusCode == 404
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {deleted: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "findFriends 4 error: #{ user.twitter_id }: #{ err }" if err
          cb null
          return

        async.forEachSeries friends, (friend, cb) ->
          models.User.update
            twitter_id: friend
          ,
            $set: {twitter_id: friend}
          ,
            upsert: true
          , (err, numberAffected, rawResponse) ->
            assert.equal numberAffected, 1 if not err
            console.error "findFriends 5 error: #{ friend }: #{ err }" if err
            cb null

        , (err) ->
          models.User.findOneAndUpdate
            twitter_id: user.twitter_id
          ,
            friends: friends
            has_friends: true
          , (err) ->
            if err
              console.error "findFriends 6 error: #{ user.twitter_id }: #{ err }"
            else
              count++
            cb null

    , (err) ->
      cb null, count

findFollowers = (cb) ->
  models.User.find
    has_followers: {$ne: true}
    in_network: true
    deleted: {$ne: true}
    private: {$ne: true}
  ,
    null
  ,
    limit: 1000
    batchSize: 100
  , (err, users) ->
    if (err)
      console.error "findFollowers 1 error: #{ err }"
      cb null, 0
      return

    count = 0
    users = _.shuffle users
    users = users[0...100]

    async.forEachSeries users, (user, cb) ->
      twitter.getFollowers user.twitter_id, (err, followers) ->
        if err
          console.error "findFollowers 2 error: #{ user.twitter_id }: #{ err }"
          if err.statusCode == 401
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {private: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "findFollowers 3 error: #{ user.twitter_id }: #{ err }" if err
          else if err.statusCode == 404
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {deleted: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "findFollowers 4 error: #{ user.twitter_id }: #{ err }" if err
          cb null
          return

        async.forEachSeries followers, (follower, cb) ->
          models.User.update
            twitter_id: follower
          ,
            $set: {twitter_id: follower}
          ,
            upsert: true
          , (err, numberAffected, rawResponse) ->
            assert.equal numberAffected, 1 if not err
            console.error "findFollowers 5 error: #{ follower }: #{ err }" if err
            cb null

        , (err) ->
          models.User.findOneAndUpdate
            twitter_id: user.twitter_id
          ,
            followers: followers
            has_followers: true
          , (err) ->
            if err
              console.error "findFollowers 6 error: #{ user.twitter_id }: #{ err }"
            else
              count++
            cb null

    , (err) ->
      cb null, count

populateUsers = (cb) ->
  models.User.find
    has_data: {$ne: true}
    deleted: {$ne: true}
  ,
    null
  ,
    limit: 50000
  , (err, users) ->
    if (err)
      console.error "populateUsers 1 error: #{ err }"
      cb null, 0
      return

    users = _.shuffle users
    users = users[0...5000]
    user_ids = (user.twitter_id for user in users)

    user_ids_grouped = []
    while user_ids.length > 0
      user_ids_grouped.push user_ids[0...100]
      user_ids = user_ids[100..]

    count = 0

    async.forEachSeries user_ids_grouped, (user_ids_100, cb) ->
      twitter.getUsers user_ids_100, (err, users) ->
        if err
          console.error "populateUsers 2 error: #{ user_ids_100 }: #{ err }"
          if err.statusCode == 404
            models.User.update
              twitter_id: {$in: user_ids_100}
            ,
              $set: {deleted: true}
            ,
              multi: true
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, user_ids_100.length if not err
              console.error "populateUsers 3 error: #{ user_ids_100 }: #{ err }" if err
          cb null
          return

        async.forEachSeries users, (user, cb) ->
          models.User.update
            twitter_id: user.id_str
          ,
            $set: {data: user, has_data: true}
          , (err, numberAffected, rawResponse) ->
            assert.equal numberAffected, 1 if not err
            if err
              console.error "populateUsers 4 error: #{ user.id_str }: #{ err }"
            else
              count++
            cb null

        , (err) ->
          cb null

    , (err) ->
      cb null, count

processTimeline = (timeline) ->
  _.map timeline, (value, key, list) ->
    # retweet_count seems to be a commulative count of all retweets globally, while favorite_count just for this particular tweet
    value = _.pick value, 'created_at', 'id_str', 'text', 'retweeted_status', 'retweet_count', 'favorite_count', 'lang', 'coordinates', 'entities'
    if value.retweeted_status
      value.is_retweet_of = value.retweeted_status.id_str
      value.is_retweet_from = value.retweeted_status.user.id_str
      delete value.retweeted_status
    if value.entities?.user_mentions
      value.entities.user_mentions = _.map value.entities.user_mentions, (user, i, l) ->
        _.pick user, 'screen_name', 'id_str'
    else if value.entities
      delete value.entities
    value

getTimeline = (cb) ->
  models.User.find
    has_timeline: {$ne: true}
    in_network: true
    deleted: {$ne: true}
    private: {$ne: true}
  ,
    null
  ,
    limit: 1000
    batchSize: 100
  , (err, users) ->
    if (err)
      console.error "getTimeline 1 error: #{ err }"
      cb null, 0
      return

    count = 0
    users = _.shuffle users
    users = users[0...100]

    async.forEachSeries users, (user, cb) ->
      twitter.getTimeline user.twitter_id, (err, timeline) ->
        if err
          console.error "getTimeline 2 error: #{ user.twitter_id }: #{ err }"
          if err.statusCode == 401
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {private: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "getTimeline 3 error: #{ user.twitter_id }: #{ err }" if err
          else if err.statusCode == 404
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {deleted: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "getTimeline 4 error: #{ user.twitter_id }: #{ err }" if err
          cb null
          return

        timeline = processTimeline timeline

        models.User.findOneAndUpdate
          twitter_id: user.twitter_id
        ,
          timeline: timeline
          has_timeline: true
        , (err) ->
          if err
            console.error "getTimeline 5 error: #{ user.twitter_id }: #{ err }"
          else
            count++
          cb null

    , (err) ->
      cb null, count

timelineToLanguages = (timeline) ->
  languages = {}
  for post in timeline
    languages[post.lang] = if languages[post.lang] then languages[post.lang] + 1 else 1
  languages

getLanguages = (cb) ->
  models.User.find
    has_languages: {$ne: true}
    deleted: {$ne: true}
    private: {$ne: true}
    has_data: true # Artificial requirement, but just to influence the order of fetching
  ,
    null
  ,
    limit: 1000
    batchSize: 100
  , (err, users) ->
    if (err)
      console.error "getLanguages 1 error: #{ err }"
      cb null, 0
      return

    count = 0
    users = _.shuffle users
    users = users[0...100]

    async.forEachSeries users, (user, cb) ->
      if user.has_timeline
        models.User.findOneAndUpdate
          twitter_id: user.twitter_id
        ,
          languages: timelineToLanguages user.timeline
          has_languages: true
        , (err) ->
          if err
            console.error "getLanguages 2 error: #{ user.twitter_id }: #{ err }"
          else
            count++
          cb null
        return

      twitter.getTimeline user.twitter_id, (err, timeline) ->
        if err
          console.error "getLanguages 3 error: #{ user.twitter_id }: #{ err }"
          if err.statusCode == 401
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {private: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "getLanguages 4 error: #{ user.twitter_id }: #{ err }" if err
          else if err.statusCode == 404
            models.User.update
              twitter_id: user.twitter_id
            ,
              $set: {deleted: true}
            , (err, numberAffected, rawResponse) ->
              assert.equal numberAffected, 1 if not err
              console.error "getLanguages 5 error: #{ user.twitter_id }: #{ err }" if err
          cb null
          return

        timeline = processTimeline timeline
        languages = timelineToLanguages timeline

        if user.in_network
          # User is in the network, but has not yet had timeline above
          models.User.findOneAndUpdate
            twitter_id: user.twitter_id
          ,
            timeline: timeline
            has_timeline: true
            languages: languages
            has_languages: true
          , (err) ->
            if err
              console.error "getLanguages 6 error: #{ user.twitter_id }: #{ err }"
            else
              count++
            cb null
        else
          models.User.findOneAndUpdate
            twitter_id: user.twitter_id
          ,
            languages: languages
            has_languages: true
          , (err) ->
            if err
              console.error "getLanguages 7 error: #{ user.twitter_id }: #{ err }"
            else
              count++
            cb null

    , (err) ->
      cb null, count

models.once 'ready', ->
  doMarkInNetwork = ->
    markInNetwork (err, count) ->
      if count > 0
        doMarkInNetwork()
      else
        _.delay doMarkInNetwork, 10000

  doPopulateUsers = ->
    populateUsers (err, count) ->
      if count > 0
        doPopulateUsers()
      else
        _.delay doPopulateUsers, 10000

  doFindFriends = ->
    findFriends (err, count) ->
      if count > 0
        doFindFriends()
      else
        _.delay doFindFriends, 10000

  doFindFollowers = ->
    findFollowers (err, count) ->
      if count > 0
        doFindFollowers()
      else
        _.delay doFindFollowers, 10000

  doGetTimeline = ->
    getTimeline (err, count) ->
      if count > 0
        doGetTimeline()
      else
        _.delay doGetTimeline, 10000

  doGetLanguages = ->
    getLanguages (err, count) ->
      if count > 0
        doGetLanguages()
      else
        _.delay doGetLanguages, 10000

  doMarkInNetwork()
  doPopulateUsers()
  doFindFriends()
  doFindFollowers()
  doGetTimeline()
  doGetLanguages()
