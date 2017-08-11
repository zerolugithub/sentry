/* eslint-env node */
/* eslint no-console:0 */
const http = require('http');
const httpProxy = require('http-proxy');

const WEBPACK_DEV_PORT = process.env.WEBPACK_DEV_PORT;
const WEBPACK_DEV_PROXY = process.env.WEBPACK_DEV_PROXY;
const SENTRY_DEVSERVER_PORT = process.env.SENTRY_DEVSERVER_PORT;

if (!WEBPACK_DEV_PORT || !WEBPACK_DEV_PROXY || !SENTRY_DEVSERVER_PORT) {
  console.error(
    'Invalid environment variables, requires: WEBPACK_DEV_PORT, WEBPACK_DEV_PROXY, SENTRY_DEVSERVER_PORT'
  );
  process.exit(1);
}

const proxy = httpProxy.createProxyServer({});
const server = http.createServer(function(req, res) {
  try {
    const matches = req.url.match(/sentry\/dist\/(.*)$/);
    if (matches && matches.length) {
      req.url = `/${matches[1]}`;
      try {
        proxy.web(req, res, {target: 'http://localhost:' + WEBPACK_DEV_PORT});
      } catch (err) {
        console.warn('`webpack-dev-server` is not started or busy');
      }
    } else {
      try {
        proxy.web(req, res, {target: 'http://localhost:' + SENTRY_DEVSERVER_PORT});
      } catch (err) {
        console.warn('sentry devserver is not started or busy');
      }
    }
  } catch (err) {
    console.log('Proxy target not responding');
  }
});
server.listen(WEBPACK_DEV_PROXY);
