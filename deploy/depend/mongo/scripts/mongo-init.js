try {
  if (!rs.status().ok) {
    rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] });
    print('replica set rs0 initialized');
  } else {
    print('replica set rs0 already initialized');
  }
} catch (e) {
  rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] });
  print('replica set rs0 initialized');
}

const userId = ObjectId('6a12bc9835bee931f30744e2');
const userCol = db.getSiblingDB('trade').getCollection('user');

if (userCol.findOne({ _id: userId }) === null) {
  userCol.insertOne({
    _id: userId,
    user_id: NumberLong('1'),
    id: NumberLong('1'),
    username: 'zhangsan',
    password: '$2a$10$RqN.PFhXEfqhbwu40TA1ce47TF39hIYwDlKsfUjBjqDPMZ0DyW55W',
    phone_number: NumberLong('13800138000'),
    status: 1,
    created_at: NumberLong('1748050000'),
    updated_at: NumberLong('1748050000'),
  });
  print('seed user zhangsan inserted');
} else {
  print('seed user zhangsan already exists');
}
