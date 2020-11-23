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


describe('Sorting tests', () => {
  it('should allow sorting on a keyword field', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?sort=sample_id:asc')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].sample_id).to.equal('C0001');
        done();
      });
  });
  it('should allow sorting on a date field', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?sort=date_performed')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].date_performed).to.equal('4/21/2020');
        done();
      });
  });
  it('should disallow sorting on a string-based analyzed field', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?sort=manufacturer')
      .end((err, res) => {
        res.should.have.status(400);
        done();
      });
  });
  it('should allow sorting on an exact field', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?sort=manufacturer.exact:desc')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].manufacturer).to.equal('W.H.P.M, Inc.');
        done();
      });
  });

});

