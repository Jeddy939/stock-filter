Set service = GetObject("winmgmts:\\.\root\cimv2")
Set processes = service.ExecQuery("SELECT ProcessId, CommandLine FROM Win32_Process WHERE CommandLine LIKE '%web_app.py%'")

stopped = 0
For Each process In processes
  commandLine = LCase(process.CommandLine)
  If InStr(commandLine, "web_app.py") > 0 Then
    process.Terminate()
    stopped = stopped + 1
  End If
Next

If stopped = 0 Then
  MsgBox "MoneyMaker was not running.", vbInformation, "MoneyMaker"
Else
  MsgBox "Stopped MoneyMaker.", vbInformation, "MoneyMaker"
End If
