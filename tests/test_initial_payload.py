# Copyright (C) DATADVANCE, 2010-2023
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Test the `initial_payload` subscription feature.

A subscription can deliver an "initial" notification to a client
immediately upon subscribing, before (and without) any call to
`Subscription.broadcast`. The payload is routed through the usual
`publish` resolver and delivered to the subscribing client only.
"""

import graphene
import pytest

import channels_graphql_ws


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_initial_payload_delivered_on_subscribe(gql, subprotocol):
    """Client receives the initial payload right after subscribing.

    No mutation/broadcast is triggered by the test: the only reason a
    notification arrives is the subscription's `initial_payload`.
    """

    initial_value = 42

    class OnValue(channels_graphql_ws.Subscription):
        """Sends an initial value on subscribe, then live updates."""

        # Send this to a client as soon as it subscribes.
        initial_payload = {"value": initial_value}

        value = graphene.Int()

        @staticmethod
        def publish(payload, info):
            """Format every notification, including the initial one."""
            del info
            return OnValue(value=payload["value"])

    class Subscription(graphene.ObjectType):
        """Root subscription."""

        on_value = OnValue.Field()

    comm = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await comm.connect_and_init()

    sub_op_id = await comm.start(
        query="subscription op_name { on_value { value } }",
        operation_name="op_name",
    )

    # The initial payload must arrive with no broadcast triggered.
    resp = await comm.receive_next(sub_op_id)
    assert resp["data"]["on_value"]["value"] == initial_value, (
        "Initial payload was not delivered on subscribe!"
    )

    # And nothing else should follow without a broadcast.
    await comm.assert_no_messages()
    await comm.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_no_initial_payload_by_default(gql, subprotocol):
    """Without `initial_payload`, nothing is sent until a broadcast.

    Guards the default behavior: the feature must be opt-in and must not
    emit a spurious notification for subscriptions that do not set it.
    """

    class OnValue(channels_graphql_ws.Subscription):
        """Plain subscription with no initial payload."""

        value = graphene.Int()

        @staticmethod
        def publish(payload, info):
            del info
            return OnValue(value=payload["value"])

    class Subscription(graphene.ObjectType):
        """Root subscription."""

        on_value = OnValue.Field()

    comm = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await comm.connect_and_init()

    await comm.start(
        query="subscription op_name { on_value { value } }",
        operation_name="op_name",
    )

    # No initial payload configured -> the client must receive nothing.
    await comm.assert_no_messages()
    await comm.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_callable_initial_payload_is_evaluated_on_subscribe(gql, subprotocol):
    """A callable `initial_payload` is evaluated per subscribe and delivered.

    The callable form lets the payload be computed from `(root, info)` at
    subscribe time (e.g. from `info.context`), so it can carry per-subscriber
    data and be routed through `publish` like any broadcast.
    """

    initial_value = 7

    class OnValue(channels_graphql_ws.Subscription):
        """Computes its initial payload from a callable."""

        value = graphene.Int()

        @staticmethod
        def initial_payload(root, info):
            """Per-subscribe initial payload (no DB, just builds a dict)."""
            del root, info
            return {"value": initial_value}

        @staticmethod
        def publish(payload, info):
            del info
            return OnValue(value=payload["value"])

    class Subscription(graphene.ObjectType):
        """Root subscription."""

        on_value = OnValue.Field()

    comm = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await comm.connect_and_init()

    sub_op_id = await comm.start(
        query="subscription op_name { on_value { value } }",
        operation_name="op_name",
    )

    resp = await comm.receive_next(sub_op_id)
    assert resp["data"]["on_value"]["value"] == initial_value, (
        "Callable initial payload was not delivered on subscribe!"
    )

    await comm.assert_no_messages()
    await comm.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_callable_initial_payload_returning_none_sends_nothing(gql, subprotocol):
    """A callable returning `None` suppresses the initial notification."""

    class OnValue(channels_graphql_ws.Subscription):
        """Callable initial payload that opts out by returning None."""

        value = graphene.Int()

        @staticmethod
        def initial_payload(root, info):
            del root, info
            return None

        @staticmethod
        def publish(payload, info):
            del info
            return OnValue(value=payload["value"])

    class Subscription(graphene.ObjectType):
        """Root subscription."""

        on_value = OnValue.Field()

    comm = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await comm.connect_and_init()

    await comm.start(
        query="subscription op_name { on_value { value } }",
        operation_name="op_name",
    )

    await comm.assert_no_messages()
    await comm.finalize()
