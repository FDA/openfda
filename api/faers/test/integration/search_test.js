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
  it('should allow wildcard search middle term', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=device:*ELIS*')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].device).to.equal('SARS-COV-2 ELISA (IgG)');
        done();
      });
  });
  it('should allow wildcard search leading term', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=device:SARS-COV-2 ELIS*')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].device).to.equal('SARS-COV-2 ELISA (IgG)');
        done();
      });
  });
  it('should allow wildcard search trailing term', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=device:*LISA (IgG)')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].device).to.equal('SARS-COV-2 ELISA (IgG)');
        done();
      });
  });
  it('should find nothing with incorrect trailing term query', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer:*omedomic')
      .end((err, res) => {
        res.should.have.status(404);
        done();
      });
  });
  it('should find nothing with incorrect leading term query', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer:Biomedome*')
      .end((err, res) => {
        res.should.have.status(404);
        done();
      });
  });
  it('should find Biomedomics with a corrected query and case insensitive', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer:*omedoMiCs')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].manufacturer).to.equal('Biomedomics');
        done();
      });
  });
  it('should find Biomedomics with a complex query', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer:*Bio*Edo*cs*+AND+device:"COVID-19 IgM-IgG Rapid Test kit"')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.results[0].manufacturer).to.equal('Biomedomics');
        done();
      });
  });
  it('should NOT find Biomedomics with a complex query if there is a spelling mistake', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer:*Bia*Edo*cs*+AND+device:"COVID-19 IgM-IgG Rapid Test kit"')
      .end((err, res) => {
        res.should.have.status(404);
        done();
      });
  });
  it('should return all results for a match-all query', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer:*')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.meta.results.total).to.equal(771);
        done();
      });
  });

});

