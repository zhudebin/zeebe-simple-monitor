package io.zeebe.monitor.rest;

import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpStatus;

/**
 * 返回数据
 */
public class R extends HashMap<String, Object> {

  private static final long serialVersionUID = 1L;

  public static final String KEY_DATA = "data";
  public static final String KEY_CODE = "code";
  public static final String KEY_MSG = "msg";

  public R() {
    put(KEY_CODE, 0);
    put(KEY_MSG, "success");
  }

  public static R error() {
    return error(HttpStatus.SC_INTERNAL_SERVER_ERROR, "未知异常，请联系管理员");
  }

  public static R error(String msg) {
    return error(HttpStatus.SC_INTERNAL_SERVER_ERROR, msg);
  }

  public static R error(int code, String msg) {
    R r = new R();
    r.put(KEY_CODE, code);
    r.put(KEY_MSG, msg);
    return r;
  }

  public static R ok(String msg) {
    R r = new R();
    r.put(KEY_MSG, msg);
    return r;
  }

  public static R ok(Map<String, Object> map) {
    R r = new R();
    r.putAll(map);
    return r;
  }

  public static R ok() {
    return new R();
  }

  @Override
  public R put(String key, Object value) {
    super.put(key, value);
    return this;
  }

  public R setData(Object value) {
    super.put(KEY_DATA, value);
    return this;
  }

  public Object getData() {
    return this.get(KEY_DATA);
  }

  public <T> T getData(Class<T> c) {
    return (T) this.get(KEY_DATA);
  }

  public int getCode() {
    return (int) this.get(KEY_CODE);
  }
}
