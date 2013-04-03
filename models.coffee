events = require 'events'
mongoose = require 'mongoose'

db = mongoose.createConnection(process.env.MONGODB_URL).on(
  'error', (err) ->
    console.error "MongoDB connection error: %s", err
    # TODO: Handle better, depending on the error?
    throw new Error "MongoDB connection error"
).once(
  'open', ->
    console.log "MongoDB connection successful"
    module.exports.emit 'ready'
)

userSchema = mongoose.Schema
  twitter_id:
    type: String
    unique: true
    required: true
  data:
    type: mongoose.Schema.Types.Mixed
    required: false
  has_data:
    type: Boolean
    index: true
    required: false
    default: false
  followers: [
    type: String
    required: false
  ]
  has_followers:
    type: Boolean
    index: true
    required: false
    default: false
  friends: [
    type: String
    required: false
  ]
  has_friends:
    type: Boolean
    index: true
    required: false
    default: false
  in_network:
    type: Boolean
    index: true
    required: false
    default: null
  deleted:
    type: Boolean
    index: true
    required: false
    default: false
  private:
    type: Boolean
    index: true
    required: false
    default: false

User = db.model 'User', userSchema

# We are setting module.exports directly because we want to use our own object for exports
module.exports = new events.EventEmitter()
module.exports.User = User
