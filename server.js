var Promise = require('bluebird');
var Winston = require('winston');
var Discogs = require('disconnect').Client;
var Plug = require('plugapi');
var YouTube = require('youtube-node');
var soundcloud = require('node-soundcloud');
var chalk = require('chalk');
var util = require('util');
var _ = require('lodash');

var CONFIG = require('./conf.js');

var bot;
var lcsBuffer = new Uint8Array(10000)
var youtube = new YouTube();
var discogs = new Discogs({
    consumerKey: CONFIG.DISCOGS_CONSUMER_KEY,
    consumerSecret: CONFIG.DISCOGS_CONSUMER_SECRET
})
    .database();

var logger = new (Winston.Logger)({
    transports: [
        new (Winston.transports.Console)({
            formatter: (options) =>
                chalk.cyan(timestamp()) + ' ' +
                chalk.green('[' + options.level.toUpperCase() + ']') + ' ' +
                (undefined !== options.message ? options.message : '') +
                (options.meta && Object.keys(options.meta).length ? '\n\t' + JSON.stringify(options.meta) : '')
        })
    ]
});

logger.warning = () =>
    logger.warn.apply(logger, arguments);

logger.success = () =>
    logger.info.apply(logger, arguments);

function twoArgPromisifier (fn) {
    return function () {
        var args = [].slice.call(arguments);
        var self = this;
        return new Promise((resolve, reject) => {
            args.push((err, res) => {
                if (err) {
                    console.log('2arg p error:', err);
                    reject(err);
                } else resolve(res);
            });
            fn.apply(self, args);
        });
    };
}

var onAdvance = Promise.coroutine(function* (data) {
    if (!data.media) return;
    if (bot.getTimeRemaining() > 1200) {
        bot.sendChat('Сорян, трекан длиннее 20 минут');
        bot.moderateRemoveDJ(bot.getDJ().id);
        return;
    }
    var cid = data.media.cid;
    var title;
    if (isNaN(+cid)) {
        var youtubeResults = yield youtube.getByIdAsync(cid);
        var snippet = youtubeResults.items[0].snippet;
        title = snippet.title.trim();
        logger.info(util.format('Youtube tags for %s: %j', title, snippet.tags));
        if (snippet.categoryId != '10') {
            logger.info(util.format(
                'Skipping "%s" due to youtube video category id mismatch (non music)', title
            ));
            bot.sendChat('Сорян, видос относится к неподходящей категории');
            bot.moderateRemoveDJ(bot.getDJ().id);
            return;
        }
        var forbiddenTag = _.find(snippet.tags, tag =>
            _.any(CONFIG.YOUTUBE_SKIPPED_TAGS, skippedTag =>
                tag.toLowerCase().indexOf(skippedTag) != -1
            )
        );
        if (forbiddenTag) {
            logger.info(util.format(
                'Skipping "%s" due to youtube match in following tag: %s', title, forbiddenTag
            ));
            bot.sendChat(util.format('Сорян, обнаружен неподходящий тэг: %s', forbiddenTag));
            bot.moderateRemoveDJ(bot.getDJ().id);
            return;
        }
    } else {
        var soundcloudResult = yield soundcloud.getAsync('/tracks/' + cid);
        title = soundcloudResult.title.trim();
        var forbiddenTag = _.find(CONFIG.YOUTUBE_SKIPPED_TAGS, skippedTag =>
            soundcloudResult.tag_list.toLowerCase().indexOf(skippedTag) != -1
        );
        if (forbiddenTag) {
            logger.info(util.format(
                'Skipping "%s" due to soundcloud match in following tag: %s', title, forbiddenTag
            ));
            bot.sendChat(util.format('Сорян, обнаружен неподходящий тэг: %s', forbiddenTag));
            bot.moderateRemoveDJ(bot.getDJ().id);
            return;
        }
    }
    var discogsResults = yield discogsFuzzySearch(title);
    if (discogsResults.length > 0) {
        logger.info('* Computing LCS lengths for "%s":', title);
        var bestMatchingRelease = _.max(discogsResults, release => _.max(
            _.map(release.tracks, trackTitle => {
                var lcslen = lcsLength(title, trackTitle);
                logger.info('| "%s" = %d', trackTitle, lcslen);
                return lcslen;
            })
        ));
        var matchSkippedGenres = _.intersection(bestMatchingRelease.genre, CONFIG.DISCOGS_SKIPPED_GENRES);
        var matchSkippedStyles = _.intersection(bestMatchingRelease.style, CONFIG.DISCOGS_SKIPPED_STYLES);
        if (matchSkippedGenres.length > 0) {
            logger.info(util.format(
                'Skipping "%s" due to discogs matches in following genres: %j', title, matchSkippedGenres
            ));
            bot.sendChat(util.format('Сорян, обнаружены неподходящие жанры: %s', matchSkippedGenres));
            bot.moderateRemoveDJ(bot.getDJ().id);
        } else if (matchSkippedStyles.length > 0) {
            logger.info(util.format(
                'Skipping "%s" due to discogs matches in following styles: %j', title, matchSkippedStyles
            ));
            bot.sendChat(util.format('Сорян, обнаружены неподходящие стили: %s', matchSkippedStyles));
            bot.moderateRemoveDJ(bot.getDJ().id);
        }
    } else {
        logger.warn('No data found for "%s" at Discogs', title);
    }
});

function discogsFuzzySearch (query) {
    var queryArtist = guessArtist(query);
    return Promise.all([
        discogs.searchAsync(query, { type: 'release' }),
        discogs.searchAsync(cleanTitle(query), { type: 'release' })
    ])
        .then(responses => _.filter(responses, r => r.results.length > 0))
        .map(response => {
            var maxLcsLen = 0;
            _.each(response.results, r => {
                r.lcsLen = lcsLength(queryArtist, guessArtist(r.title));
                if (r.lcsLen > maxLcsLen)
                    maxLcsLen = r.lcsLen;
            });
            return _.filter(response.results, r => r.lcsLen == maxLcsLen).slice(0, 2);
        })
        .then(entries => _.uniq(_.flatten(entries), e => e.id))
        .map(entry => discogs.releaseAsync(entry.id).then(release => {
            var releaseArtists = joinArtists(release.artists);
            var tracklist = _.filter(release.tracklist, t => t.title && t.title.trim().length > 0);
            return {
                genre: entry.genre,
                style: entry.style,
                tracks: _.map(tracklist, entry =>
                    (entry.artists ? joinArtists(entry.artists) : releaseArtists) + ' - ' + entry.title
                )
            };
        }))
        .catch(retry('discogsFuzzySearch', arguments));
}

function retry (label, args) {
    return function (e) {
        logger.error('Error during `%s`, retrying...', label);
        return args.callee.apply(this, args);
    };
}

function joinArtists (artists) {
    return _.foldl(artists,
        (acc, x) => util.format('%s%s%s', acc, x.name, x.join),
        ''
    );
}

function cleanTitle (string) {
    var newString = '';
    var depth = 0;
    for (var i = 0; i < string.length; ++i) {
        var c = string.charAt(i);
        if (c == '(' || c == '[' || c == '{') {
            depth++;
        } else if (c == ')' || c == ']' || c == '}') {
            if (depth > 0) {
                depth--;
                continue;
            }
        }
        if (depth == 0) {
            newString += c;
        }
    }
    return newString
        .replace(/official\s*video/gi, '')
        .replace(/official/gi, '')
        .replace(/full\s*album/gi, '')
        .replace(/full/gi, '')
        .replace(/live at.*/gi, '')
        .replace(/live in.*/gi, '')
        .replace(/(^|[^A-Za-z])hd([^A-Za-z]|$)/gi, '')
        .replace(/(^|[^A-Za-z])hq([^A-Za-z]|$)/gi, '')
        .replace(/(\d+)p/gi, '');
}

function strCutSpaces (string) {
    return string.replace(/\s+/g, '');
}

function pad (n) {
    return n < 10 ? '0' + n.toString(10) : n.toString(10);
}

var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function timestamp () {
    var d = new Date();
    var time = [pad(d.getHours()), pad(d.getMinutes()), pad(d.getSeconds())].join(':');
    return [pad(d.getDate()), months[d.getMonth()], time].join(' ');
}

function guessArtist (title) {
    return title.substr(0, title.indexOf(' - '));
}

function lcsLength (a, b) {
    var m = a.length;
    var n = b.length;
    for (var i = 0; i <= m; ++i) {
        for (var j = 0; j <= n; ++j) {
            if (i == 0 || j == 0)
                lcsBuffer[i * (n + 1) + j] = 0;
            else if (a[i-1] == b[j-1])
                lcsBuffer[i * (n + 1) + j] = lcsBuffer[(i - 1) * (n + 1) + j - 1] + 1;
            else {
                var x = lcsBuffer[(i - 1) * (n + 1) + j];
                var y = lcsBuffer[i * (n + 1) + j - 1];
                lcsBuffer[i * (n + 1) + j] = x > y ? x : y;
            }
        }
    }
    return lcsBuffer[(m + 1) * (n + 1) - 1];
}

youtube.setKey(CONFIG.YOUTUBE_API_KEY);
soundcloud.init({ id: CONFIG.SOUNDCLOUD_CLIENT_ID });

Promise.promisifyAll(discogs, { promisifier: twoArgPromisifier });
Promise.promisifyAll(youtube, { promisifier: twoArgPromisifier });
Promise.promisifyAll(soundcloud, { promisifier: twoArgPromisifier });

Plug.prototype.setLogger(logger);

bot = new Plug({
    email: CONFIG.PLUG_BOT_EMAIL,
    password: CONFIG.PLUG_BOT_PASSWORD
});

bot.on('advance', onAdvance);
bot.on('close', () => bot.connect(CONFIG.ROOM));
bot.on('error', () => bot.connect(CONFIG.ROOM));

bot.connect(CONFIG.ROOM);
