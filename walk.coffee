async = require 'async'

_ = require 'underscore'

location = require './location'
models = require './models'
twitter = require './twitter'

markInNetwork = (cb) ->
  models.User.find
    in_network: null
    deleted: {$ne: true}
    data: {$ne: null}
  , (err, users) ->
    if (err)
      console.error "markInNetwork 1 error: #{ err }"
      cb null, 0
      return

    count = 0

    async.forEach users, (user, cb) ->
      in_network = user.data.time_zone == 'Ljubljana' or location.REGEX.test(user.data.location) or false
      models.User.findOneAndUpdate
        twitter_id: user.twitter_id
      ,
        in_network: in_network
      , (err) ->
        console.error "markInNetwork 2 error: #{ user.twitter_id }: #{ err }" if err
        count++ if in_network
        cb null

    , (err) ->
      cb null, count

findFriends = (cb) ->
  models.User.find
    friends: null
    in_network: true
    deleted: {$ne: true}
  , (err, users) ->
    if (err)
      console.error "findFriends 1 error: #{ err }"
      cb null, 0
      return

    count = 0

    async.forEach users, (user, cb) ->
      twitter.getFriends user.twitter_id, (err, friends) ->
        if err
          console.error "findFriends 2 error: #{ user.twitter_id }: #{ err }"
          cb null
          return

        async.forEach friends, (friend, cb) ->
          models.User.findOneAndUpdate
            twitter_id: friend
          ,
            twitter_id: friend
          ,
            upsert: true
            new: false # We set "new" to false because of this bug: https://github.com/mongodb/node-mongodb-native/issues/699
          , (err) ->
            console.error "findFriends 3 error: #{ friend }: #{ err }" if err
            cb null

        , (err) ->
          models.User.findOneAndUpdate
            twitter_id: user.twitter_id
          ,
            friends: friends
          , (err) ->
            if err
              console.error "findFriends 4 error: #{ user.twitter_id }: #{ err }"
            else
              count++
            cb null

    , (err) ->
      cb null, count

findFollowers = (cb) ->
  models.User.find
    followers: null
    in_network: true
    deleted: {$ne: true}
  , (err, users) ->
    if (err)
      console.error "findFollowers 1 error: #{ err }"
      cb null, 0
      return

    count = 0

    async.forEach users, (user, cb) ->
      twitter.getFollowers user.twitter_id, (err, followers) ->
        if err
          console.error "findFollowers 2 error: #{ user.twitter_id }: #{ err }"
          cb null
          return

        async.forEach followers, (follower, cb) ->
          models.User.findOneAndUpdate
            twitter_id: follower
          ,
            twitter_id: follower
          ,
            upsert: true
            new: false # We set "new" to false because of this bug: https://github.com/mongodb/node-mongodb-native/issues/699
          , (err) ->
            console.error "findFollowers 3 error: #{ follower }: #{ err }" if err
            cb null

        , (err) ->
          models.User.findOneAndUpdate
            twitter_id: user.twitter_id
          ,
            followers: followers
          , (err) ->
            if err
              console.error "findFollowers 4 error: #{ user.twitter_id }: #{ err }"
            else
              count++
            cb null

    , (err) ->
      cb null, count

populateUsers = (cb) ->
  models.User.find
    data: null
    deleted: {$ne: true}
  , (err, users) ->
    if (err)
      console.error "populateUsers 1 error: #{ err }"
      cb null, 0
      return

    user_ids = (user.twitter_id for user in users)

    user_ids_grouped = []
    while user_ids.length > 0
      user_ids_grouped.push user_ids[0...100]
      user_ids = user_ids[100..]

    count = 0

    async.forEach user_ids_grouped, (user_ids_100, cb) ->
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
              console.error "populateUsers 3 error: #{ user_ids_100 }: #{ err }" if err
          cb null
          return

        async.forEach users, (user, cb) ->
          models.User.findOneAndUpdate
            twitter_id: user.id_str
          ,
            data: user
          ,
            upsert: true
            new: false # We set "new" to false because of this bug: https://github.com/mongodb/node-mongodb-native/issues/699
          , (err) ->
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
  markInNetworkCount = 1
  populateUsersCount = 1
  findFriendsCount = 1
  findFollowersCount = -1

  doMarkInNetwork = ->
    markInNetwork (err, count) ->
      markInNetworkCount = count
      if markInNetworkCount + populateUsersCount + findFriendsCount + findFollowersCount > 0
        _.delay doMarkInNetwork, 5000

  doPopulateUsers = ->
    populateUsers (err, count) ->
      populateUsersCount = count
      if markInNetworkCount + populateUsersCount + findFriendsCount + findFollowersCount > 0
        _.delay doPopulateUsers, 5000

  doFindFriends = ->
    findFriends (err, count) ->
      findFriendsCount = count
      if markInNetworkCount + populateUsersCount + findFriendsCount + findFollowersCount > 0
        _.delay doFindFriends, 5000

  doFindFollowers = ->
    findFollowers (err, count) ->
      findFollowersCount = count
      if markInNetworkCount + populateUsersCount + findFriendsCount + findFollowersCount > 0
        _.delay doFindFollowers, 5000

  doMarkInNetwork()
  doPopulateUsers()
  doFindFriends()
  doFindFollowers()
