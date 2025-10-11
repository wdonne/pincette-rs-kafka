package net.pincette.rs.kafka;

public class MessageLagException extends RuntimeException {
  public MessageLagException(final String message) {
    super(message);
  }
}
