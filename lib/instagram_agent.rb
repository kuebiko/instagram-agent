require 'instagram'
require 'timers'

class InstagramAgent < Kuebiko::Agent
  MEDIA_RESOURCE_TOPIC = 'resources/instagram/media'
  PERSONA_RESOURCE_TOPIC = 'entities/persona'

  SOURCE = 'Instagram API'
  MEDIA_RESOURCE_TYPE = 'instagram.media'

  attr_accessor :rest_client, :stream_client

  def initialize
    super

    @client = Instagram.client
    @timers = Timers::Group.new

    @last_id = nil

    # Send credentials request and register callback
    puts "Subscribed to : #{agent_control_topics.join(', ')}"

    puts 'Requesting credentials now'
    request_credentials
  end

  def handle_new_media(media)
    msg = Kuebiko::Message.new send_to: [MEDIA_RESOURCE_TOPIC]

    msg.payload = Kuebiko::MessagePayload::Document.new.tap do |pl|
      pl.agent_type = self.class.name

      pl.created_at = media.created_time

      if media.type == "image"
        pl.mime_type = "image/jpeg"
        pl.content   = media.images.standard_resolution.url
      else
        pl.mime_type = "video/mp4"
        pl.content   = media.videos.standard_resolution
      end

      pl.type      = MEDIA_RESOURCE_TYPE
      pl.source    = SOURCE
      pl.source_id = media.id

      pl.keywords  = media.tags
      pl.metadata  = media.to_h
    end

    dispatcher.send(msg)
  end

  def update_user_feed
    p "Calling user_media_feed"
    media = @client.user_media_feed(count: 1000, min_id: @last_id)

    p "Got #{media.size} media items"

    media.each { |m| handle_new_media(m) }

    @last_id = media[0].id if media.size > 0

    p "Next batch starts from #{@last_id}"
  end

  def start_pooling
    puts 'Let\'s get this show on the road'
    @timers.every(30) do
      puts 'wake up'
      update_user_feed
    end

    loop { @timers.wait }
  end

  def request_credentials
    msg = Kuebiko::Message.new send_to: ['resources/configurations']
    msg.payload = Kuebiko::MessagePayload::Query.new(query: :instagram)

    dispatcher.send(msg, method(:handle_credentials_reply))
  end

  def handle_credentials_reply(msg)
    config = JSON.parse(msg.payload.body, symbolize_names: true)

    @client.client_id     = config[:client_id]
    @client.client_secret = config[:client_secret]
    @client.access_token  = config[:oauth_token]

    start_pooling
  rescue JSON::ParserError
    # TODO: Proper log this you idiot
    puts 'Invalid message payload'
  end
end
