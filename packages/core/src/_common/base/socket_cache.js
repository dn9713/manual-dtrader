import moment from 'moment';
import { isEmptyObject, getPropertyValue, LocalStore } from '@deriv/shared';
import { getStaticHash } from '_common/utility';

/*
 * Caches WS responses to reduce delay time and number of requests
 * Stores data in LocalStore which is the first one available in: localStorage, sessionStorage, InScriptStore
 */
const SocketCache = (() => {
    const config = {
        proposal_open_contract: { expire: 10 },
        contracts_for: { expire: 10 },
        trading_times: { expire: 120 },
    };

    const storage_key = 'ws_cache';
    let data_obj = {};

    const msg_type_mapping = {
        history: 'ticks_history',
        candles: 'ticks_history',
    };

    const set = (key, response) => {
        const msg_type = msg_type_mapping[response.msg_type] || response.msg_type;
        if (response.subscription) {
            const can_cache = msg_type === 'proposal_open_contract' && response.proposal_open_contract.is_sold;
            if (!can_cache) return;
        }

        if (response?.echo_req?.end === 'latest') return;
        if (!config[msg_type]) return;

        const cached_response = get(response.echo_req) || {};
        const cached_message = cached_response[msg_type];
        const new_message = response[msg_type];

        const has_error_or_missing = response.error;
        const has_new_value = cached_message && isEmptyValue(cached_message) && !isEmptyValue(new_message);
        const has_old_cache = cached_message && isEmptyValue(new_message) && !isEmptyValue(cached_message);
        const has_valid_cache = !isEmptyValue(cached_response) && !cached_response.error;

        if ((has_error_or_missing || has_new_value || has_old_cache) && has_valid_cache) {
            clear();
            return;
        }

        const expires = moment().add(config[msg_type].expire, 'm').valueOf();

        if (!data_obj.static_hash) {
            data_obj.static_hash = getStaticHash();
        }

        data_obj[key] = { value: response, expires, msg_type };
        LocalStore.setObject(storage_key, data_obj);
    };

    const isEmptyValue = data => {
        let is_empty_data = false;
        if (Array.isArray(data)) {
            if (!data.length) is_empty_data = true;
        } else if (typeof data === 'object' && data !== null) {
            if (!Object.keys(data).length) is_empty_data = true;
        }
        return is_empty_data;
    };

    const reloadDataObj = () => {
        if (isEmptyObject(data_obj)) {
            data_obj = LocalStore.getObject(storage_key);
            if (isEmptyObject(data_obj)) return;
        }

        if (data_obj.static_hash !== getStaticHash()) {
            clear();
        }
    };

    const getData = key => getPropertyValue(data_obj, key) || {};

    const get = key => {
        reloadDataObj();
        const response_obj = getData(key);
        let response;
        if (moment().isBefore(response_obj.expires)) {
            response = response_obj.value;
        } else {
            remove(key);
        }
        return response;
    };

    const getByMsgType = msg_type => {
        reloadDataObj();
        const key = Object.keys(data_obj).find(k => getData(k).msg_type === msg_type);
        if (!key) return undefined;
        const response_obj = getData(key);
        let response;
        if (moment().isBefore(response_obj.expires)) {
            response = response_obj.value;
        } else {
            remove(key);
        }
        return response;
    };

    const has = key => !!get(key);

    const remove = (key, should_match_all) => {
        if (should_match_all) {
            Object.keys(data_obj).forEach(data_key => {
                if (data_key.indexOf(key) !== -1) {
                    delete data_obj[data_key];
                }
            });
        } else if (key in data_obj) {
            delete data_obj[key];
        }
        LocalStore.setObject(storage_key, data_obj);
    };

    const clear = () => {
        LocalStore.remove(storage_key);
        data_obj = {};
    };

    return {
        set,
        get,
        getByMsgType,
        has,
        remove,
        clear,
    };
})();

export default SocketCache;
