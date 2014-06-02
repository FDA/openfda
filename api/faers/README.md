# FAERS API

## Getting started

1. Get an ES instance running locally with data in the FAERS mapping format.

2. Install node

3. Setup node modules

```
npm install
```

4. Start node

```
node api.js
```

5. In another window

```
curl localhost:8000/drug/event.json?search=humira
```
