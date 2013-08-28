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
  timeline: [
    type: mongoose.Schema.Types.Mixed
    required: false
  ]
  has_timeline:
    type: Boolean
    index: true
    required: false
    default: false
  languages:
    type: mongoose.Schema.Types.Mixed
    required: false
  has_languages:
    type: Boolean
    index: true
    required: false
    default: false
  user_mentions:
    type: mongoose.Schema.Types.Mixed
    required: false
  has_user_mentions:
    type: Boolean
    index: true
    required: false
    default: false
  retweets:
    type: mongoose.Schema.Types.Mixed
    required: false
  has_retweets:
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

userSchema.index 'deleted': 1, 'has_data': 1
userSchema.index 'in_network': 1, 'deleted': 1, 'has_data': 1
userSchema.index 'has_timeline': 1, 'in_network': 1, 'deleted': 1, 'private': 1
userSchema.index 'has_languages': 1, 'deleted': 1, 'private': 1, 'has_data': 1

User = db.model 'User', userSchema

# We are setting module.exports directly because we want to use our own object for exports
module.exports = new events.EventEmitter()
module.exports.User = User
