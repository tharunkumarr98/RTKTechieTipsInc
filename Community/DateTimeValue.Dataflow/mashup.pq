[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Data = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45WMjDUByIjAyNTJR0lAwMrAwMgbQgkY3XQJQ1hkoZYJY1BksZYdRpBdBqZQCWNsNhpZo5NEmqnqSk2SSOInSYmxkqxsQA=", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Date = _t, Time = _t, Value = _t]),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Date", type date}, {"Time", type text}, {"Value", Int64.Type}})
in
  #"Changed column type";
shared Data_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "9b5477b5-6be3-4988-8af1-c46817471564"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "51291887-2700-4091-8856-c6da00e483dd"]}[Data],
  TableNavigation = Navigation_2{[Id = "Data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
