models = require './models'

processUsers = (users) ->
  for user in users
    models.User.findOneAndUpdate
      twitter_id: user.id_str
    ,
      data: user
    ,
      upsert: true
      new: false # We set "new" to false because of this bug: https://github.com/mongodb/node-mongodb-native/issues/699
    , (err) ->
      console.error err if err

  process.exit 0

importUsers = ->
  buffer = ''
  process.stdin.resume()
  process.stdin.on(
    'data', (chunk) -> buffer += chunk
  ).on(
    'end', -> processUsers JSON.parse buffer
  )

models.once 'ready', ->
  importUsers()
