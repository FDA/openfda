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


describe('GET /device/covid19serology.json', () => {
  it('should return first page of 100 hits with a search-after Next link to the 2nd page', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer.exact:Euroimmun&limit=100')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res).to.have.header('Link');
        expect(res.body.results).to.have.lengthOf(100);

        const linkHeader = res.get('Link');
        expect(parseLinkHeader(linkHeader, true).next.search_after).to.not.be.empty;

        chai
          .request(app)
          .get(parseNextURL(linkHeader))
          .end((err, res) => {
            res.should.have.status(200);
            expect(res).to.not.have.header('Link');
            expect(res.body.results).to.have.lengthOf(10);
            done();
          });
      });
  });

  it('should properly page through a small result set', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=days_from_symptom:17&sort=manufacturer.exact:asc&limit=4')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res).to.have.header('Link');
        expect(res.body.results).to.have.lengthOf(4);

        // Verify the actual content of the first page
        expect(res.body.results.map(rec => rec.manufacturer)).to.deep.equal(['Biomedomics', 'Euroimmun', 'Hangzhou Biotest Biotech, Co., Ltd.', 'Healgen']);

        const linkHeader = res.get('Link');
        const next = parseLinkHeader(linkHeader, true).next;
        expect(next.search_after).to.not.be.empty;
        expect(next.skip).to.equal('0')
        expect(next.limit).to.equal('4')
        expect(next.search).to.equal('days_from_symptom:17')
        expect(next.sort).to.equal('manufacturer.exact:asc')

        chai
          .request(app)
          .get(parseNextURL(linkHeader))
          .end((err, res) => {
            res.should.have.status(200);
            expect(res).to.not.have.header('Link');
            expect(res.body.results).to.have.lengthOf(3);
            // Verify the actual content of the 2nd page
            expect(res.body.results.map(rec => rec.manufacturer)).to.deep.equal(['Phamatech', 'Tianjin Beroni Biotechnology Co., Ltd.', 'W.H.P.M, Inc.']);

            done();
          });
      });
  });

  it('should properly page through a small result set, but without search_after if skip is used', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=days_from_symptom:17&sort=manufacturer.exact:asc&limit=3&skip=1')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res).to.have.header('Link');
        expect(res.body.results).to.have.lengthOf(3);

        // Verify the actual content of the first page
        expect(res.body.results.map(rec => rec.manufacturer)).to.deep.equal(['Euroimmun', 'Hangzhou Biotest Biotech, Co., Ltd.', 'Healgen']);

        const linkHeader = res.get('Link');
        const next = parseLinkHeader(linkHeader, true).next;
        expect(next.search_after).to.be.undefined;
        expect(next.skip).to.equal('4')
        expect(next.limit).to.equal('3')
        expect(next.search).to.equal('days_from_symptom:17')
        expect(next.sort).to.equal('manufacturer.exact:asc')

        chai
          .request(app)
          .get(parseNextURL(linkHeader))
          .end((err, res) => {
            res.should.have.status(200);
            expect(res).to.not.have.header('Link');
            expect(res.body.results).to.have.lengthOf(3);
            // Verify the actual content of the 2nd page
            expect(res.body.results.map(rec => rec.manufacturer)).to.deep.equal(['Phamatech', 'Tianjin Beroni Biotechnology Co., Ltd.', 'W.H.P.M, Inc.']);

            done();
          });
      });
  });

  it('should error out when skip and search_after are used together', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?search=manufacturer.exact:Euroimmun&skip=1&search_after=0=covid123')
      .end((err, res) => {
        res.should.have.status(400);
        expect(res.body.error.message).to.equal('The skip parameter is not supported when using search_after.')
        done();
      });
  });

  it('should not include Link header for count request', done => {
    chai
      .request(app)
      .get('/device/covid19serology.json?count=manufacturer.exact&limit=100')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res).to.not.have.header('Link');
        done();
      });
  });
});

var parseNextURL = (link) => {
  const url = new Url(parseLinkHeader(link).next.url);
  return url.pathname + url.query;
}
