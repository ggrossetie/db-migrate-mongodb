'use strict'

exports.up = async function (db) {
  await db.createCollection('pets')
  await db.addIndex('pets', 'name_1', ['name'], true)
  await db.insert('pets', { name: 'Rex' })
}

exports.down = async function (db) {
  await db.dropCollection('pets')
}

exports._meta = {
  version: 1
}
