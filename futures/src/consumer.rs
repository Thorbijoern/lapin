use futures_core::Stream;
use lapin_async::consumer::ConsumerSubscriber;
use tokio_io::{AsyncRead,AsyncWrite};
use std::collections::VecDeque;
use std::io;
use std::mem::PinMut;
use std::sync::{Arc,Mutex};
use std::task::{self,Poll};

use message::Delivery;
use transport::*;

#[derive(Clone,Debug)]
pub struct ConsumerSub {
  inner: Arc<Mutex<ConsumerInner>>,
}

impl ConsumerSubscriber for ConsumerSub {
  fn new_delivery(&mut self, delivery: Delivery) {
    trace!("new_delivery;");
    if let Ok(mut inner) = self.inner.lock() {
      inner.deliveries.push_back(delivery);
      /*
      if let Some(ctx) = inner.ctx.as_ref() {
        ctx.waker().wake();
      }
      */
    } else {
      // FIXME: what do we do here?
      error!("new_delivery; mutex error");
    }
  }
}

#[derive(Clone)]
pub struct Consumer<T> {
  transport:    Arc<Mutex<AMQPTransport<T>>>,
  inner:        Arc<Mutex<ConsumerInner>>,
  channel_id:   u16,
  queue:        String,
  consumer_tag: String,
}

#[derive(Debug)]
struct ConsumerInner {
  deliveries: VecDeque<Delivery>,
  //ctx:        Option<PinMut<'static, task::Context<'static>>>,
}

impl Default for ConsumerInner {
  fn default() -> Self {
    Self {
      deliveries: VecDeque::new(),
      //ctx:        None,
    }
  }
}

impl<T: AsyncRead+AsyncWrite+Sync+Send+'static> Consumer<T> {
  pub fn new(transport: Arc<Mutex<AMQPTransport<T>>>, channel_id: u16, queue: String, consumer_tag: String) -> Consumer<T> {
    Consumer {
      transport,
      inner: Arc::new(Mutex::new(ConsumerInner::default())),
      channel_id,
      queue,
      consumer_tag,
    }
  }

  pub fn update_consumer_tag(&mut self, consumer_tag: String) {
    self.consumer_tag = consumer_tag;
  }

  pub fn subscriber(&self) -> ConsumerSub {
    ConsumerSub {
      inner: self.inner.clone(),
    }
  }
}

impl<T: AsyncRead+AsyncWrite+Sync+Send+'static> Stream for Consumer<T> {
  type Item = Result<Delivery, io::Error>;

  fn poll_next(self: PinMut<Self>, ctx: &mut task::Context) -> Poll<Option<Self::Item>> {
    trace!("consumer poll; consumer_tag={:?} polling transport", self.consumer_tag);
    let mut transport = lock_transport!(self.transport, ctx);
    transport.poll_next(ctx)?;
    let mut inner = match self.inner.lock() {
      Ok(inner) => inner,
      Err(_)    => if self.inner.is_poisoned() {
        return Err(io::Error::new(io::ErrorKind::Other, "Consumer mutex is poisoned"))
      } else {
        ctx.waker().wake();
        return Poll::Pending;
      },
    };
    trace!("consumer poll; consumer_tag={:?} acquired inner lock", self.consumer_tag);
    /*
    if inner.ctx.is_none() {
      ctx.waker().wake();
      inner.ctx = Some(PinMut::new(ctx));
    }
    */
    if let Some(delivery) = inner.deliveries.pop_front() {
      trace!("delivery; consumer_tag={:?} delivery_tag={:?}", self.consumer_tag, delivery.delivery_tag);
      Poll::Ready(Ok(Some(delivery)))
    } else {
      trace!("delivery; consumer_tag={:?} status=Pending", self.consumer_tag);
      Poll::Pending
    }
  }
}
