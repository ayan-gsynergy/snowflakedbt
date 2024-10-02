Barani tasks:
1. Improvement on removing duplicate CTEs
2. Improve if similar rule is present in 2 different measure groups - I think we should not think about it at all
3. Variable when to generate SQL - Parse Tree

Discussed:
1. Measure with no rule - add as null coz if measure has default and if presentation layer wants to get it then without having the column even the presentation layer will not be able to get its default value so should have the measure column created -- Needs more thought. For now dont create column. And if there is a single measure in a measure group then dont even create a table.


To Do:
1. Try it 
2. How to aggregate multiple measures from a measure group via CTEs into 1 SQL model
3. Read GenAI must ask questions