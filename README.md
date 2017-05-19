BidderBot
=========

BidderBot places buy "bids" and sell "asks" according to some
simple rules:

* not within the spread between the highest bid and lowest ask
* such that the ask will net a minimum of profit
* applying compound "profit" exponential growth curve

This will gradually convert quote currency (e.g., USD) into many
separate exchanges into base currency (e.g., Ethereum or Bitcoin) with
"limit bids".  As soon as the bid market goes up with legit looking
competitive bids, BidderBot will cancel and place a similarly priced
bid.

It periodically adjusts the objective sell price for each purchases
according the above exponential growth curve rule.  The purchase(s)
with the lowest objective are placed with a "limit ask".  Ask orders
are also cancelled and replaced as they grow.

## Dependencies

This requires a Redis database, a Coinbase GDAX secret/key/passphrase
that can trade, and some initial GDAX funds (~$10).

## Warning

This is merely an investment experiment/toy.  Use at your own risk.
