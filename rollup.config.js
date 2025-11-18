export default {
  input: 'index.js',
  output: {
    file: 'bundle.cjs',
    format: 'cjs',
  },
  external: ['mongodb', 'db-migrate-base', 'bluebird']
};
