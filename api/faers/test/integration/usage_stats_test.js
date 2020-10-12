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


describe('GET /usage.json', () => {
  it('should include download statistics', done => {
    chai
      .request(app)
      .get('/usage.json')
      .end((err, res) => {
        res.should.have.status(200);
        expect(res.body.downloadStats).to.not.be.undefined;
        expect(res.body.downloadStats.deviceudi).to.equal(34);
        expect(res.body.downloadStats.deviceenforcement).to.equal(26);
        expect(res.body.downloadStats.devicereglist).to.equal(10);
        expect(res.body.downloadStats.druglabel).to.equal(157);
        expect(res.body.downloadStats.devicepma).to.equal(5);
        expect(res.body.downloadStats.foodenforcement).to.equal(64);
        expect(res.body.downloadStats.foodevent).to.equal(27);
        expect(res.body.downloadStats.deviceevent).to.equal(583);
        expect(res.body.downloadStats.deviceclass).to.equal(22);
        expect(res.body.downloadStats.devicerecall).to.equal(22);
        expect(res.body.downloadStats.drugenforcement).to.equal(40);
        expect(res.body.downloadStats.drugevent).to.equal(2360);

        done();
      });
  });

});

