How to get the app hash code?

1. Deploy the dao in example/dao of algo-builder repo.
2. If dao is successfully deployed, then make note of app id. (We assume you got app id = 7, we will use this
app id in next step).
3. Open the pgdb database of indexer.
4. Run below query:
`SELECT params FROM app WHERE index=7;`
5. You should be able to see json structure. You will find `approv` filed in this json structure.
6. Paste the `approv` field from json structure to `SigmaDAOApp.txt` file.
