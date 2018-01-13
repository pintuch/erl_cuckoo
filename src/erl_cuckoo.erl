-module(erl_cuckoo).

%% API exports
-export([new/0, 
         new/1,
         insert/2,   
         lookup/2,
         delete/2]).

-define(ALL1s(Size), ((1 bsl Size) - 1)).

-define(FNV64_PRIME, 1099511628211).
-define(FNV64_INIT, 14695981039346656037).
-define(FNV64_MASK, 16#FFFFFFFFFFFFFFFF).

%%====================================================================
%% Default filter configuration
-define(DEFAULT_FINGERPRINT_SIZE,           16).
-define(DEFAULT_BUCKET_SIZE,                4).
-define(DEFAULT_CAPACITY,                   10000).
-define(DEFAULT_EVICTIONS,                  1000).
%%====================================================================

-record(cuckoo_filter, {
    buckets             :: array:array(),
    fingerprint_size    = ?DEFAULT_FINGERPRINT_SIZE :: non_neg_integer(),
    bucket_size         = ?DEFAULT_BUCKET_SIZE :: non_neg_integer(),
    max_evictions       = ?DEFAULT_EVICTIONS :: non_neg_integer()
    % hash1_fun           = fun h1/1 :: fun((term())-> non_neg_integer()),
    % hash2_fun           = fun h2/1 :: fun((non_neg_integer())-> non_neg_integer())
}).

-opaque cuckoo_filter_dt() :: #cuckoo_filter{}.
-export_type([cuckoo_filter_dt/0]).

%%====================================================================
%% API functions
%%====================================================================
-spec new() -> cuckoo_filter_dt().
new() ->
    new([]).

-spec proplists:proplist() -> cuckoo_filter_dt().
new(Opts) ->
    Capacity        = proplists:get_value(capacity, Opts, ?DEFAULT_CAPACITY),
    FingerPrintSize = proplists:get_value(fingerprint_size, Opts, ?DEFAULT_FINGERPRINT_SIZE),
    BucketEntries   = proplists:get_value(bucket_size, Opts, ?DEFAULT_BUCKET_SIZE),
    MaxEvictions    = proplists:get_value(max_evictions, Opts, ?DEFAULT_EVICTIONS),
    % Hash1Fun        = proplists:get_value(hash1_fun, Opts, fun h1/1),
    % Hash2Fun        = proplists:get_value(hash2_fun, Opts, fun h2/1),
    NumBuckets      = get_next_pow2(Capacity / BucketEntries),
    Buckets         = array:new(NumBuckets, [{default, erl_cuckoo_buckets:new(BucketEntries)}]),
    #cuckoo_filter{
        fingerprint_size    = FingerPrintSize,
        bucket_size         = BucketEntries,
        buckets             = Buckets,
        max_evictions       = MaxEvictions
        % hash1_fun           = Hash1Fun,
        % hash2_fun           = Hash2Fun
    }.

%%====================================================================
%% Internal functions
%%====================================================================
-spec ceil(number()) -> integer().
ceil(N) ->
    Truncated = erlang:trunc(N),
    case Truncated < N of
        true -> Truncated + 1;
        false -> Truncated
    end.

-spec log2(number()) -> number().
log2(N) ->
    math:log(N) / math:log(2).

-spec get_next_pow2(number()) -> non_neg_integer().
get_next_pow2(N) ->
    erlang:trunc(math:pow(2, ceil(log2(N)))).

-spec fnv64a(Data :: binary()) -> non_neg_integer().
fnv64a(Data) ->
    fnv64a(Data, ?FNV64_INIT).
         
fnv64a(<<H:8, T/bytes>>, State) ->
    Hash  =  ((State bxor H) * ?FNV64_PRIME) band ?FNV64_MASK,
    fnv64a(T, Hash);   
fnv64a(<<>>, State) ->
    State.

%%--------------------------------------------------------------------
%% @doc The first hash function takes an arbitrary term and produces
%%      an integral output
%% @end
%%--------------------------------------------------------------------
-spec h1(Data :: term()) -> non_neg_integer().
h1(Data) ->
    fnv64a(term_to_binary(Data)).

%%--------------------------------------------------------------------
%% @doc The first hash function takes an arbitrary term and produces
%%      an integral output
%% @end
%%--------------------------------------------------------------------

-spec h2(Data :: non_neg_integer()) -> non_neg_integer().
h2(Data) ->
    erlang:phash2(Data).

-spec get_fingerprint_index1(Data, FingerPrintSize, NumBuckets) -> Value when
    Data            :: term(),
    FingerPrintSize :: non_neg_integer(),
    NumBuckets      :: non_neg_integer(),    
    Index1          :: non_neg_integer(),
    FingerPrint     :: non_neg_integer(),    
    Value           :: tuple(FingerPrint, Index1).
get_fingerprint_index1(Data, FingerPrintSize, NumBuckets) ->
    Hash1 = h1(Data),
    % The fingerprint must not exceed the FingerPrintSize
    FingerPrint = Hash1 band ?ALL1s(FingerPrintSize),
    % Do a modulo, the index must be less than the number of buckets
    Index1 = Hash1 rem NumBuckets,
    {FingerPrint, Index1}.

-spec get_index2(Index1, FingerPrint, NumBuckets) -> Index2 when
    Index1      :: non_neg_integer(),
    FingerPrint :: erl_cuckoo_buckets:fingerprint(),
    NumBuckets  :: non_neg_integer(),
    Index2      :: non_neg_integer().
get_index2(Index1, FingerPrint, NumBuckets) ->
    Hash2 = h2(FingerPrint),
    Index1 bxor (Hash2 rem NumBuckets).

-spec get_fingerprint_and_indices(Data, FingerPrintSize, NumBuckets) -> Value when
    Data            :: term(),
    FingerPrintSize :: non_neg_integer(),
    NumBuckets      :: non_neg_integer(),    
    Index1          :: non_neg_integer(),
    Index2          :: non_neg_integer(),
    FingerPrint     :: erl_cuckoo_buckets:fingerprint(),
    Value           :: tuple(FingerPrint, Index1, Index2).
get_fingerprint_and_indices(Data, FingerPrintSize, NumBuckets) ->
    {FingerPrint, Index1} = get_fingerprint_index1(Data, FingerPrintSize, NumBuckets),
    Index2 = get_index2(Index1, FingerPrint, NumBuckets),
    {FingerPrint, Index1, Index2}.

-spec lookup(Data, cuckoo_filter_dt()) -> boolean() when
    Data :: term().
lookup(Data, CuckooFilter) ->
    #cuckoo_filter{buckets = Buckets, fingerprint_size = FingerPrintSize} = CuckooFilter,
    NumBuckets = array:size(Buckets),
    {FingerPrint, Index1, Index2} = 
        get_fingerprint_and_indices(Data, FingerPrintSize, NumBuckets),
    % LookupIndex gets the relevant bucket for the index and checks if the fingerprint
    % exists in any of the bucket's slots
    LookupIndex = fun(Index) ->
        Bucket = array:get(Index, Buckets),
        case erl_cuckoo_buckets:find(FingerPrint, Bucket) of
            {true, _Index} -> true;
            false -> false
        end
    end,    
    LookupIndex(Index1) orelse LookupIndex(Index2).
    
-spec insert(term(), cuckoo_filter_dt()) -> cuckoo_filter_dt() | false.
insert(Data, CuckooFilter) ->
    #cuckoo_filter{
        buckets             = Buckets, 
        fingerprint_size    = FingerPrintSize,
        max_evictions       = MaxEvictions
    } = CuckooFilter,
    NumBuckets = array:size(Buckets),            
    {FingerPrint, Index1, Index2} = 
        get_fingerprint_and_indices(Data, FingerPrintSize, NumBuckets),

    case insert_at_index(Index1, FingerPrint, CuckooFilter) of
        false ->
            case insert_at_index(Index2, FingerPrint, CuckooFilter) of
                false ->
                    force_insert_at_index(Index2, FingerPrint, CuckooFilter, MaxEvictions);
                CuckooFilter1 ->
                    CuckooFilter1
            end;
        CuckooFilter1 ->
            CuckooFilter1
    end.

-spec force_insert_at_index(Index, FingerPrint, CuckooFilter1, RetriesLeft) -> 
        CuckooFilter2 | false when
    Index           :: non_neg_integer(),
    FingerPrint     :: erl_cuckoo_buckets:fingerprint(),
    RetriesLeft     :: non_neg_integer(),
    CuckooFilter1   :: cuckoo_filter_dt(),
    CuckooFilter2   :: cuckoo_filter_dt().
force_insert_at_index(Index, FingerPrint, CuckooFilter, 0) ->
    insert_at_index(Index, FingerPrint, CuckooFilter);
force_insert_at_index(Index, FingerPrint, CuckooFilter, RetriesLeft) when RetriesLeft > 0 ->
    #cuckoo_filter{buckets = Buckets} = CuckooFilter,
    NumBuckets = size(Buckets),
    % Get the bucket for the index
    Bucket0 = array:get(Index, Buckets),
    % Force-insert after evicting the old fingerprint
    {EvictedFingerPrint, Bucket1} = erl_cuckoo_buckets:force_insert(FingerPrint, Bucket0),
    % Set the new bucket in the filter
    Buckets1 = array:set(Index, Bucket1, Buckets),
    CuckooFilter1 = CuckooFilter#cuckoo_filter{buckets = Buckets1},
    % Get the new home for the evicted FingerPrint
    NewIndex = get_index2(Index, EvictedFingerPrint, NumBuckets),
    force_insert_at_index(NewIndex, EvictedFingerPrint, CuckooFilter1, RetriesLeft - 1).

-spec insert_at_index(Index, FingerPrint, CuckooFilter1) -> CuckooFilter2 | false when
    Index           :: non_neg_integer(),
    FingerPrint     :: erl_cuckoo_buckets:fingerprint(),
    CuckooFilter1   :: cuckoo_filter_dt(),
    CuckooFilter2   :: cuckoo_filter_dt().
insert_at_index(Index, FingerPrint, CuckooFilter) ->
    #cuckoo_filter{buckets = Buckets} = CuckooFilter,
    Bucket = array:get(Index, Buckets),
    case erl_cuckoo_buckets:insert(FingerPrint, Bucket) of
        {true, Bucket1} ->
            Buckets1 = array:set(Index, Bucket1, Buckets),
            CuckooFilter#cuckoo_filter{buckets = Buckets1};
        false -> false
    end.
%%--------------------------------------------------------------------
%% @doc If data is absent, definitely returns false;
%%      For the other case, may return a new cuckoo filter
%% @end 
%%--------------------------------------------------------------------
-spec delete(Data, CuckooFilter) -> false | CuckooFilter1 when
    Data            :: term(),
    CuckooFilter    :: cuckoo_filter_dt(),
    CuckooFilter1   :: cuckoo_filter_dt().
delete(Data, CuckooFilter) ->
    #cuckoo_filter{buckets = Buckets, fingerprint_size = FingerPrintSize} = CuckooFilter,
    NumBuckets = array:size(Buckets),
    {FingerPrint, Index1, Index2} = 
        get_fingerprint_and_indices(Data, FingerPrintSize, NumBuckets),
    DeleteAtIndex = fun(Index) ->
        Bucket = array:get(Index, Buckets),
        case erl_cuckoo_buckets:delete(FingerPrint, Bucket) of
            {true, Bucket1} ->
                Buckets1 = array:set(Index, Bucket1, Buckets),
                CuckooFilter#cuckoo_filter{buckets = Buckets1};
            false -> false
        end
    end,    
    case DeleteAtIndex(Index1) of
        false -> DeleteAtIndex(Index2);
        CuckooFilter1 -> CuckooFilter1
    end.