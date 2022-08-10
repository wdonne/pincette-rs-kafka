module net.pincette.rs.kafka {
  requires net.pincette.common;
  requires net.pincette.rs;
  requires net.pincette.rs.streams;
  requires kafka.clients;
  requires java.logging;
  requires java.json;

  opens net.pincette.rs.kafka;
  exports net.pincette.rs.kafka;
}
