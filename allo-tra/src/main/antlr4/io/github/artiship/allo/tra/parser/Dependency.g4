grammar Dependency;

expr
    : dependencyRule dependencyRange
    ;

dependencyRule
    : ruleType (PARENTHESES_START N PARENTHESES_END)?
    ;

dependencyRange
    : bracketStart rangeTimeFormat COMMA range bracketEnd EOF
    ;

rangeTimeFormat
    :  year '-' month '-' day (' ') hour ':' minute ':' second
    ;

year
    : YYYY
    ;

month
    : MM
    | TIME_TRUNCATED
    ;

day
    : DD
    | TIME_TRUNCATED
    ;

hour
    : HH
    | TIME_TRUNCATED
    ;

minute
    : M
    | TIME_TRUNCATED
    ;

second
    : S
    | TIME_TRUNCATED
    ;

range
    : rangeStart (COMMA rangeEnd)?
    ;

rangeStart
    : offset+
    ;

rangeEnd
    : offset+
    ;

offset
    : timeUnit PARENTHESES_START operation? N PARENTHESES_END
    ;

bracketStart
    : PARENTHESES_START
    | SQUARE_START
    ;

bracketEnd
    : PARENTHESES_END
    | SQUARE_END
    ;

ruleType
    : ALL
    | ANY
    | FIRST
    | LAST
    | CONTINUES
    ;

timeUnit
    : SECONDS
    | MINUTES
    | HOURS
	| DAYS
    | WEEKS
    | MONTHS
    | YEARS
    ;

operation
    : MINUS
    | PLUS
    | NONE
    ;

MINUS: '-';
PLUS: '+';
NONE: ' ';

PARENTHESES_START: '(';
SQUARE_START: '[';
PARENTHESES_END: ')';
SQUARE_END: ']';

ALL: '*';
ANY: 'A';
FIRST: 'F';
LAST: 'L';
CONTINUES: 'C';

SECONDS: 's';
MINUTES: 'm';
HOURS: 'h';
DAYS: 'd';
WEEKS: 'w';
MONTHS: 'M';
YEARS: 'y';

YYYY: 'yyyy';
MM: 'MM';
DD: 'dd';
HH: 'HH';
M: 'mm';
S: 'ss';

COMMA: ',' (' ')?;

N
    : '0'
    | [1-9] [0-9]*
    ;

TIME_TRUNCATED
    : '00'
    ;