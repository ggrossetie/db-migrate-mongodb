export default {
  entry: 'index.js',
  dest: 'bundle.cjs',
  format: 'cjs',
  external: ['mongodb', 'db-migrate-base', 'bluebird']
};
