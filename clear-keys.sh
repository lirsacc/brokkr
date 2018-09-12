redis-cli EVAL "return redis.call('del', unpack(redis.call('keys', ARGV[1])))" 0 brokkr:*
