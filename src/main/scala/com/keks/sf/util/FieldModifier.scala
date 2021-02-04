package com.keks.sf.util

import java.lang.reflect.Method


object FieldModifier {

  def setMethodAccessible(obj: Object, name: String, parameters: Class[_]*): Method = {
    val m: Method = obj.getClass.getDeclaredMethod(name, parameters:_*)
    m.setAccessible(true)
    m
  }

}
