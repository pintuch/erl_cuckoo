-module(erl_cuckoo_buckets).
-export([
    new/1,
    insert/2,
    force_insert/2,
    delete/2,
    find/2
]).

-type fingerprint() :: non_neg_integer().
-type bucket(A)     :: array:array(A).
-opaque bucket()    :: bucket(fingerprint()).
-export_type([bucket/0,
              fingerprint/0]).

-spec new(non_neg_integer()) -> bucket().
new(Size) ->
    array:new(Size).

-spec insert(FingerPrint, Bucket1) -> Result when
    FingerPrint :: fingerprint(),
    Bucket1     :: bucket(),
    Bucket2     :: bucket(),
    Result      :: tuple(true, Bucket2) | false.
insert(FingerPrint, Bucket) ->
    case find(undefined, Bucket) of
        {true, Index} -> {true, array:set(Index, FingerPrint, Bucket)};
        false -> false
    end.

-spec force_insert(FingerPrint, Bucket) -> tuple(OldFingerPrint, Bucket1) when
    FingerPrint     :: fingerprint(),
    OldFingerPrint  :: fingerprint(),
    Bucket          :: bucket(),
    Bucket1         :: bucket().
force_insert(FingerPrint, Bucket) ->
    BucketSize      = array:size(Bucket),
    Index           = random:uniform(BucketSize) - 1,
    OldFingerPrint  = array:get(Index, Bucket),
    Bucket1         = array:set(Index, FingerPrint, Bucket),
    {OldFingerPrint, Bucket1}.

-spec delete(FingerPrint, Bucket1) -> Result when
    FingerPrint :: fingerprint(),
    Bucket1     :: bucket(),
    Bucket2     :: bucket(),
    Result      :: tuple(true, Bucket2) | false.
delete(FingerPrint, Bucket) ->
    case find(FingerPrint, Bucket) of
        {true, Index} -> {true, array:unset(Index, Bucket)};
        false -> false
    end.

-spec find(FingerPrint, Bucket) -> Result when
    FingerPrint :: fingerprint(),
    Bucket      :: bucket(),
    Index       :: non_neg_integer(),
    Result      :: tuple(true, Index) | false.
find(FingerPrint, Bucket) ->
    {Found, Index} = array:foldl(
        fun (I, V, {false, _}) when V == FingerPrint ->
                {true, I};
            (_I, _V, Acc) ->
                Acc
        end, {false, 0}, Bucket
    ),
    case Found of
        false -> false;
        true -> {Found, Index}
    end.
