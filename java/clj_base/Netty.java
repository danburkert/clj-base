package clj_base;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;

/**
 * Wrapper class for calls to Netty 4 classes which inherit from non-public
 * abstract classes.  Necessary because of a bug in Clojure (CLJ-1183).
 * Credit to scramjet (https://gist.github.com/scramjet/5606195).
 */
public class Netty
{

  // Bootstrap Methods
  public static Bootstrap channel (Bootstrap bootstrap, Class<? extends Channel> channelClass) {
    return bootstrap.channel(channelClass);
  }

  public static Bootstrap group (Bootstrap bootstrap, EventLoopGroup group) {
    return bootstrap.group(group);
  }

  public static Bootstrap handler (Bootstrap bootstrap, ChannelHandler handler) {
    return bootstrap.handler(handler);
  }

  // ChannelHandlerContext Methods
  public static ByteBufAllocator alloc (ChannelHandlerContext ctx) {
    return ctx.alloc();
  }

  public static ChannelPipeline pipeline (ChannelHandlerContext ctx) {
    return ctx.pipeline();
  }

  public static ChannelFuture writeAndFlush (ChannelHandlerContext ctx, Object msg) {
    return ctx.writeAndFlush(msg);
  }
}
