Note: Below steps needs to be done in testing environment.

Please follow below steps to get latest app hash code:

1. Print `ApprovalProgram` in writer.go of this repo inside writeAppResource() method.
`fmt.Println(b64.EncodeToString([]byte(resource.Params.Params.ApprovalProgram)))`
You can use above to print the `ApprovalProgram` in human readable form.
2. Deploy the dao in example/dao of algo-builder repo.
3. Open the console of indexer.
4. Observe console, you will see long string. This is the app hash code. Copy this hash code.
5. Paste this code to `SigmaDAOApp.txt` file.

Your app hash code is updated now.
