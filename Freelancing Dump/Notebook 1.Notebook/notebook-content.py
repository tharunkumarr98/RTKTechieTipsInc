# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

let
    scrappedHtml = List.Transform(Text.Split(Text.AfterDelimiter(Text.BetweenDelimiters(Text.BetweenDelimiters(Text.BetweenDelimiters(Text.FromBinary(Web.Contents("https://data.sca.isr.umich.edu/tables.php#")), "The Index of Consumer Sentiment", "                 </div>")," href=", ">"), """",""""), "get-table.php?"),"&"), each Text.AfterDelimiter(_,"=") ),
    c =  scrappedHtml{0},
    y =  scrappedHtml{1},
    m =  scrappedHtml{2},
    n =  scrappedHtml{3},
    f =  scrappedHtml{4},
    k =  scrappedHtml{5},
    Source = Pdf.Tables(Web.Contents("https://data.sca.isr.umich.edu/get-table.php", [Query= [
                 #"c"= c,
                 #"y"= y , 
                 #"m"= m, 
                 #"n"= n, 
                 #"f"= f, 
                 #"k"= k
                 ]]), [Implementation="1.3"]),
    Table002 = Source{[Id="Table002"]}[Data]
in
    Table002

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
