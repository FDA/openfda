// Usage stats endpoint tests.
const chai = require('chai');
const chaiHttp = require('chai-http');
chai.use(chaiHttp);
const app = require('../../api');
const should = chai.should();
const expect = chai.expect;
const parseLinkHeader = require('parse-link-header');
const Url = require('url-parse');
const querystring = require('querystring');


describe('Search tests', () => {
  it('should permit the & character within search query', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer.exact:"Test %26 Co"')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].sample_id).to.equal('C0999');
        done();
      });
  });

});

