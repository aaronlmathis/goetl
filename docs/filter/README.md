<!-- Code generated by gomarkdoc. DO NOT EDIT -->

# filter

```go
import "github.com/aaronlmathis/goetl/filter"
```

## Index

- [func And\(filters ...goetl.Filter\) goetl.Filter](<#And>)
- [func Between\(field string, min, max float64\) goetl.Filter](<#Between>)
- [func Contains\(field, substring string\) goetl.Filter](<#Contains>)
- [func EndsWith\(field, suffix string\) goetl.Filter](<#EndsWith>)
- [func Equals\(field string, expectedValue interface\{\}\) goetl.Filter](<#Equals>)
- [func GreaterThan\(field string, threshold float64\) goetl.Filter](<#GreaterThan>)
- [func In\(field string, values ...interface\{\}\) goetl.Filter](<#In>)
- [func LessThan\(field string, threshold float64\) goetl.Filter](<#LessThan>)
- [func MatchesRegex\(field, pattern string\) goetl.Filter](<#MatchesRegex>)
- [func Not\(filter goetl.Filter\) goetl.Filter](<#Not>)
- [func NotNull\(field string\) goetl.Filter](<#NotNull>)
- [func Or\(filters ...goetl.Filter\) goetl.Filter](<#Or>)
- [func StartsWith\(field, prefix string\) goetl.Filter](<#StartsWith>)


<a name="And"></a>
## func [And](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L192>)

```go
func And(filters ...goetl.Filter) goetl.Filter
```

And creates a filter that requires all provided filters to pass

<a name="Between"></a>
## func [Between](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L158>)

```go
func Between(field string, min, max float64) goetl.Filter
```

Between creates a filter that includes records where the numeric field is between min and max \(inclusive\)

<a name="Contains"></a>
## func [Contains](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L66>)

```go
func Contains(field, substring string) goetl.Filter
```

Contains creates a filter that includes records where the string field contains the substring

<a name="EndsWith"></a>
## func [EndsWith](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L94>)

```go
func EndsWith(field, suffix string) goetl.Filter
```

EndsWith creates a filter that includes records where the string field ends with the suffix

<a name="Equals"></a>
## func [Equals](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L55>)

```go
func Equals(field string, expectedValue interface{}) goetl.Filter
```

Equals creates a filter that includes records where the field equals the specified value

<a name="GreaterThan"></a>
## func [GreaterThan](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L123>)

```go
func GreaterThan(field string, threshold float64) goetl.Filter
```

GreaterThan creates a filter that includes records where the numeric field is greater than the value

<a name="In"></a>
## func [In](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L175>)

```go
func In(field string, values ...interface{}) goetl.Filter
```

In creates a filter that includes records where the field value is in the provided set

<a name="LessThan"></a>
## func [LessThan](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L141>)

```go
func LessThan(field string, threshold float64) goetl.Filter
```

LessThan creates a filter that includes records where the numeric field is less than the value

<a name="MatchesRegex"></a>
## func [MatchesRegex](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L108>)

```go
func MatchesRegex(field, pattern string) goetl.Filter
```

MatchesRegex creates a filter that includes records where the string field matches the regex pattern

<a name="Not"></a>
## func [Not](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L224>)

```go
func Not(filter goetl.Filter) goetl.Filter
```

Not creates a filter that negates the provided filter

<a name="NotNull"></a>
## func [NotNull](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L38>)

```go
func NotNull(field string) goetl.Filter
```

NotNull creates a filter that excludes records where the specified field is nil or empty

<a name="Or"></a>
## func [Or](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L208>)

```go
func Or(filters ...goetl.Filter) goetl.Filter
```

Or creates a filter that requires at least one of the provided filters to pass

<a name="StartsWith"></a>
## func [StartsWith](<https://github.com/aaronlmathis/goetl/blob/main/filter/filters.go#L80>)

```go
func StartsWith(field, prefix string) goetl.Filter
```

StartsWith creates a filter that includes records where the string field starts with the prefix

Generated by [gomarkdoc](<https://github.com/princjef/gomarkdoc>)
