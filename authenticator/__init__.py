import threading
import pika


def callback(channel, method, headers, body):
    """

    :param channel:
    :param method:
    :param headers:
    :param body:
    :return:
    """
    pass

def start_authenticator(connection):
    """

    :param connection:
    :return:
    """
    try:
        t1 = threading.Thread(
            target=_consume,
            args=(connection, ),
            name='consumer'
        )
        t1.start()
    except Exception as e:
        print(f'\nError in authenticator.start_authenticator(): \n{str(e)}')

def _consume(connection):
    """

    :param connection:
    :return:
    """
    try:
        channel = connection.channel()
        channel.exchange_declare(exchange='gestion_interna.rpc', exchange_type='direct', durable=False)
        channel.queue_declare(queue='gestion_interna.rpc', exclusive=True, durable=False)
        channel.queue_bind(
            exchange='gestion_interna.rpc',
            queue='gestion_interna.rpc',
            routing_key='gestion_interna.rpc'
        )
        channel.basic_consume(queue='gestion_interna.rpc', on_message_callback=callback)
        print('\nEscuchando mensajes de autenticación en modo RPC...')
        channel.start_consuming()
        return channel
    except Exception as e:
        print(f'\nError in sender.initialize_consumer_with_thread(): \n{str(e)}')
        return None


def confirm(channel, method, headers, body):
    """

    :param body:
    :param channel:
    :param headers:
    :param method:
    :return:
    """
    _respond(channel, headers, method, body)


def deny(channel, method, headers):
    """

    :param channel:
    :param headers:
    :param method:
    :return:
    """
    body = ''
    _respond(channel, headers, method, body)


def _respond(channel, headers, method, body):
    """

    :param channel:
    :param headers:
    :param method:
    :param body:
    :return:
    """
    channel.basic_publish(exchange='',
                          routing_key=headers.reply_to,
                          properties=pika.BasicProperties(correlation_id = \
                                                         headers.correlation_id),
                          body=body)
    channel.basic_ack(delivery_tag=method.delivery_tag)
