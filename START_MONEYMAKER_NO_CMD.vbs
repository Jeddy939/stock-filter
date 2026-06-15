Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
launcher = projectDir & "\START_MONEYMAKER.bat"

If Not fso.FileExists(launcher) Then
  MsgBox "Could not find START_MONEYMAKER.bat in:" & vbCrLf & projectDir, vbCritical, "MoneyMaker"
  WScript.Quit 1
End If

shell.CurrentDirectory = projectDir
shell.Run """" & launcher & """", 0, False
