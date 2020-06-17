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


describe('CORS Tests', () => {
  it('preflight response should include correct headers', done => {
    chai
      .request(app)
      .options('/device/covid19serology.json')
      .end((err, res) => {
        res.should.have.status(204);
        expect(res).to.have.header('Access-Control-Allow-Origin');
        expect(res).to.have.header('Access-Control-Allow-Credentials');
        expect(res).to.have.header('Access-Control-Allow-Headers');
        expect(res).to.have.header('Access-Control-Allow-Methods');

        expect(res.get('Access-Control-Allow-Origin')).to.equal('*')
        expect(res.get('Access-Control-Allow-Credentials')).to.equal('true')
        expect(res.get('Access-Control-Allow-Methods')).to.include('GET')
          .and.include('HEAD')
        expect(res.get('Access-Control-Allow-Headers')).to.include('X-Requested-With')
          .and.include('Authorization')
          .and.include('Content-Type')
          .and.include('Upgrade-Insecure-Requests')
        done();
      });
  });
  it('data endpoint responses continue to include needed CORS headers', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json')
      .end((err, res) => {
        assertCorsHeaders(res);
        done();
      });
  });

  function assertCorsHeaders(res) {
    res.should.have.status(200);
    expect(res).to.have.header('Access-Control-Allow-Origin');
    expect(res).to.have.header('Access-Control-Allow-Credentials');

    expect(res.get('Access-Control-Allow-Origin')).to.equal('*')
    expect(res.get('Access-Control-Allow-Credentials')).to.equal('true')
  }

  it('status endpoint responses continue to include needed CORS headers', done => {
    chai
      .request(app)
      .get('/status')
      .end((err, res) => {
        assertCorsHeaders(res);
        done();
      });
  });


  it('usage endpoint responses continue to include needed CORS headers', done => {
    chai
      .request(app)
      .get('/usage.json')
      .end((err, res) => {
        assertCorsHeaders(res);
        done();
      });
  });


});

