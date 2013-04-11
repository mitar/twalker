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
  count = 0

  models.User.find(
    has_followers: true
  ).batchSize(10).stream().on('error', (err) ->
    console.error "addFollowersAndFriends 1 error: #{ err }"
    cb null, 0
    return
  ).on('data', (user) ->
    async.forEach user.followers, (follower, cb) ->
      models.User.update
        twitter_id: follower
      ,
        $set: {twitter_id: follower}
      ,
        upsert: true
      , (err, numberAffected, rawResponse) ->
        assert.equal numberAffected, 1 if not err
        console.error "addFollowersAndFriends 2 error: #{ follower }: #{ err }" if err
        cb null
  ).on('close', ->
    models.User.find(
      has_friends: true
    ).batchSize(10).stream().on('error', (err) ->
      console.error "addFollowersAndFriends 3 error: #{ err }"
      cb null, 0
      return
    ).on('data', (user) ->
      async.forEach user.friends, (friend, cb) ->
        models.User.update
          twitter_id: friend
        ,
          $set: {twitter_id: friend}
        ,
          upsert: true
        , (err, numberAffected, rawResponse) ->
          assert.equal numberAffected, 1 if not err
          console.error "addFollowersAndFriends 4 error: #{ friend }: #{ err }" if err
          cb null
    ).on('close', ->
      cb null, count
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

        async.forEach friends, (friend, cb) ->
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

        async.forEach followers, (follower, cb) ->
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
    limit: 1000
    batchSize: 100
  , (err, users) ->
    if (err)
      console.error "populateUsers 1 error: #{ err }"
      cb null, 0
      return

    users = _.shuffle users
    users = users[0...100]
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

        async.forEach users, (user, cb) ->
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

  doMarkInNetwork()
  doPopulateUsers()
  doFindFriends()
  doFindFollowers()
