// API Integration tests
// API runs against a Dockerized Elasticsearch instance with at least one index.
const chai = require('chai');
const chaiHttp = require('chai-http');
chai.use(chaiHttp);
const app = require('../../api');
const should = chai.should();
const expect = chai.expect;
const parseLinkHeader = require('parse-link-header');
const Url = require('url-parse');
const querystring = require('querystring');


describe('Disabled endpoint tests', () => {
  it('a disabled endpoint should bounce incoming API requests', done => {
    chai
      .request(app)
      .get('/other/historicaldocument.json')
      .end((err, res) => {
        res.should.have.status(404);
        done();
      });
  });

  it('status should not have mentioning of the disabled endpoint', done => {
    chai
      .request(app)
      .get('/status')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.find(e => e.endpoint === "otherhistoricaldocument")).to.be.undefined;
        done();
      });
  });


});

