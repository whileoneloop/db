////////////////////////// GET SERVER TIME DIFFERENCE //////////////////////////

const ServerTime = {};

// XXX: TODO use a http rest point instead - creates less overhead
Meteor.methods({
  'getServerTime': function() {
    return Date.now();
  }
});

// Unify client / server api
ServerTime.now = function() {
  return Date.now();
};

export default ServerTime;
