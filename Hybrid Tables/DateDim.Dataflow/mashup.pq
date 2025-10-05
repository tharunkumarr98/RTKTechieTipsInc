[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimDate_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimDate = let
    CurrentDate = Date.From(DateTimeZone.FixedUtcNow()),
    StartDateKey = 20200101,
    EndDateKey = 20261231,
    StartDate = #date(Number.Round(StartDateKey/10000,0), Number.Round(Number.Mod(StartDateKey,10000)/100,0),  Number.Mod(Number.Mod(StartDateKey,10000),100) )  ,
    EndDate = #date(Number.Round(EndDateKey/10000,0), Number.Round(Number.Mod(EndDateKey,10000)/100,0),  Number.Mod(Number.Mod(EndDateKey,10000),100) )  ,
    NumberOfDays = Duration.Days(EndDate-StartDate) + 1,
    Source = Table.FromList(List.Dates(StartDate,NumberOfDays,#duration(1,0,0,0)), Splitter.SplitByNothing(),{"Date"} ),
    ChangedDateColumnType = Table.TransformColumnTypes(Source,{{"Date", type date}}),
    AddedDateKey = Table.AddColumn(ChangedDateColumnType, "DateKey", each Number.From(Date.ToText([Date], [Format="yyyyMMdd"])), Int64.Type),
    AddedYear = Table.AddColumn(AddedDateKey, "Year", each Date.Year([Date]), Int64.Type),
    AddedYearOffset = Table.AddColumn(AddedYear, "YearOffset", each Date.Year([Date]) - Date.Year(CurrentDate), Int64.Type),
    AddedMonth = Table.AddColumn(AddedYearOffset, "MonthNumber", each Date.Month([Date]), Int64.Type),
    AddedMonthName = Table.AddColumn(AddedMonth, "MonthName", each Date.MonthName([Date]), type text),
    AddedYearMonthSort = Table.AddColumn(AddedMonthName, "YearMonthSort", each [Year] * 100 + [MonthNumber], Int64.Type),
    AddedYearMonth = Table.AddColumn(AddedYearMonthSort, "YearMonth", each Text.From([Year]) & "-" & Text.Range([MonthName],0,3), type text),
    AddedMonthOffset = Table.AddColumn(AddedYearMonth, "MonthOffsetNumber", each ((12 * Date.Year([Date])) + [MonthNumber]) - ((12 * Date.Year(CurrentDate)) + Date.Month(CurrentDate)) , Int64.Type),
    AddedQuarterNumber = Table.AddColumn(AddedMonthOffset, "QuarterNumber", each Date.QuarterOfYear([Date]), Int64.Type),
    AddedQuarterText = Table.AddColumn(AddedQuarterNumber, "QuarterName", each "Q" & Text.From([QuarterNumber]), type text),
    AddedYearQuarterSort = Table.AddColumn(AddedQuarterText, "YearQuarterSort", each [Year] * 100 + [QuarterNumber], Int64.Type),
    AddedYearQuarter = Table.AddColumn(AddedYearQuarterSort, "YearQuarter", each Text.From([Year]) & "-" & [QuarterName], type text),
    AddedQuarterOffset = Table.AddColumn(AddedYearQuarter, "QuarterOffset", each ((4 * Date.Year([Date])) + [QuarterNumber]) - ((4 * Date.Year(CurrentDate)) + Date.QuarterOfYear(CurrentDate)) , Int64.Type),
    AddedWeekNumber = Table.AddColumn(AddedQuarterOffset, "WeekNumber", each Date.WeekOfYear([Date]), Int64.Type),
    AddedWeekName = Table.AddColumn(AddedWeekNumber, "WeekName", each "W" & Text.From([WeekNumber]), type text),
    AddedYearWeekSort = Table.AddColumn(AddedWeekName, "YearWeekSort", each [Year] * 100 + [WeekNumber], Int64.Type),
    AddedYearweek = Table.AddColumn(AddedYearWeekSort, "YearWeek", each Text.From([Year]) & "-" & [WeekName], type text),
    AddedStartOfWeek = Table.AddColumn(AddedYearweek, "StartOfWeek", each Date.StartOfWeek([Date]), type date),
    AddedWeekOffset = Table.AddColumn(AddedStartOfWeek, "WeekOffset", each (Number.From([StartOfWeek]) - Number.From(Date.StartOfWeek(CurrentDate))) / 7, Int64.Type)
in
    AddedWeekOffset;
shared DimDate_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "5bb7e532-b8dc-4144-986f-8d0e5112c4ee"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "6bff0765-0ffd-4c03-be0e-eb908c63053d"]}[Data],
  TableNavigation = Navigation_2{[Id = "DimDate", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
