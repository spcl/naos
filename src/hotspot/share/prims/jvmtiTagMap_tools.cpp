#include "precompiled.hpp"
#include "classfile/javaClasses.inline.hpp"
#include "classfile/symbolTable.hpp"
#include "classfile/systemDictionary.hpp"
#include "classfile/vmSymbols.hpp"
#include "code/codeCache.hpp"
#include "jvmtifiles/jvmtiEnv.hpp"
#include "logging/log.hpp"
#include "memory/allocation.inline.hpp"
#include "memory/resourceArea.hpp"
#include "oops/access.inline.hpp"
#include "oops/arrayOop.inline.hpp"
#include "oops/constantPool.inline.hpp"
#include "oops/instanceMirrorKlass.hpp"
#include "oops/objArrayKlass.hpp"
#include "oops/objArrayOop.inline.hpp"
#include "oops/oop.inline.hpp"
#include "oops/typeArrayOop.inline.hpp"
#include "prims/jvmtiEventController.hpp"
#include "prims/jvmtiEventController.inline.hpp"
#include "prims/jvmtiExport.hpp"
#include "prims/jvmtiImpl.hpp"
#include "prims/jvmtiTagMap.hpp"
#include "runtime/biasedLocking.hpp"
#include "runtime/frame.inline.hpp"
#include "runtime/handles.inline.hpp"
#include "runtime/javaCalls.hpp"
#include "runtime/jniHandles.inline.hpp"
#include "runtime/mutex.hpp"
#include "runtime/mutexLocker.hpp"
#include "runtime/reflectionUtils.hpp"
#include "runtime/thread.inline.hpp"
#include "runtime/threadSMR.hpp"
#include "runtime/vframe.hpp"
#include "runtime/vmThread.hpp"
#include "runtime/vm_operations.hpp"
#include "utilities/macros.hpp"
#include "utilities/queue.hpp"
#include "utilities/globalDefinitions.hpp"
#if INCLUDE_ZGC
#include "gc/z/zGlobals.hpp"
#endif
#include "jvmtiTagMap_tools.hpp"

ClassFieldMap::ClassFieldMap() {
  _fields = new (ResourceObj::C_HEAP, mtInternal)
          GrowableArray<ClassFieldDescriptor*>(initial_field_count, true);
}

ClassFieldMap::~ClassFieldMap() {
  for (int i=0; i<_fields->length(); i++) {
    delete _fields->at(i);
  }
  delete _fields;
}

void ClassFieldMap::add(int index, char type, int offset) {
  ClassFieldDescriptor* field = new ClassFieldDescriptor(index, type, offset);
  _fields->append(field);
}

// Returns a heap allocated ClassFieldMap to describe the static fields
// of the given class.
//
ClassFieldMap* ClassFieldMap::create_map_of_static_fields(Klass* k) {
  HandleMark hm;
  InstanceKlass* ik = InstanceKlass::cast(k);

  // create the field map
  ClassFieldMap* field_map = new ClassFieldMap();

  FilteredFieldStream f(ik, false, false);
  int max_field_index = f.field_count()-1;

  int index = 0;
  for (FilteredFieldStream fld(ik, true, true); !fld.eos(); fld.next(), index++) {
    // ignore instance fields
    if (!fld.access_flags().is_static()) {
      continue;
    }
    field_map->add(max_field_index - index, fld.signature()->byte_at(0), fld.offset());
  }
  return field_map;
}

// Returns a heap allocated ClassFieldMap to describe the instance fields
// of the given class. All instance fields are included (this means public
// and private fields declared in superclasses and superinterfaces too).
//
ClassFieldMap* ClassFieldMap::create_map_of_instance_fields(oop obj) {
  HandleMark hm;
  InstanceKlass* ik = InstanceKlass::cast(obj->klass());

  // create the field map
  ClassFieldMap* field_map = new ClassFieldMap();

  FilteredFieldStream f(ik, false, false);

  int max_field_index = f.field_count()-1;

  int index = 0;
  for (FilteredFieldStream fld(ik, false, false); !fld.eos(); fld.next(), index++) {
    // ignore static fields
    if (fld.access_flags().is_static()) {
      continue;
    }
    field_map->add(max_field_index - index, fld.signature()->byte_at(0), fld.offset());
  }

  return field_map;
}

ClassFieldMap* ClassFieldMap::create_map_of_instance_obj_fields(oop obj) {
  HandleMark hm;
  InstanceKlass* ik = InstanceKlass::cast(obj->klass());

  // create the field map
  ClassFieldMap* field_map = new ClassFieldMap();

  FilteredFieldStream f(ik, false, false);

  int max_field_index = f.field_count()-1;

  int index = 0;
  for (FilteredFieldStream fld(ik, false, false); !fld.eos(); fld.next(), index++) {
    char type = fld.signature()->byte_at(0);
    // ignore static fields
    if (fld.access_flags().is_static()) {
      continue;
    }
    if (type != 'L' && type != '[') {
      continue;
    }
    field_map->add(max_field_index - index, fld.signature()->byte_at(0), fld.offset());
  }

  return field_map;
}


GrowableArray<InstanceKlass*>* JvmtiCachedClassFieldMap::_class_list;

JvmtiCachedClassFieldMap::JvmtiCachedClassFieldMap(ClassFieldMap* field_map) {
  _field_map = field_map;
}

JvmtiCachedClassFieldMap::~JvmtiCachedClassFieldMap() {
  if (_field_map != NULL) {
    delete _field_map;
  }
}

bool ClassFieldMapCacheMark::_is_active;


// record that the given InstanceKlass is caching a field map
void JvmtiCachedClassFieldMap::add_to_class_list(InstanceKlass* ik) {
  if (_class_list == NULL) {
    _class_list = new (ResourceObj::C_HEAP, mtInternal)
            GrowableArray<InstanceKlass*>(initial_class_count, true);
  }
  _class_list->push(ik);
}

// returns the instance field map for the given object
// (returns field map cached by the InstanceKlass if possible)
ClassFieldMap* JvmtiCachedClassFieldMap::get_map_of_instance_fields(oop obj) {
  assert(Thread::current()->is_VM_thread(), "must be VMThread");
  assert(ClassFieldMapCacheMark::is_active(), "ClassFieldMapCacheMark not active");

  Klass* k = obj->klass();
  InstanceKlass* ik = InstanceKlass::cast(k);

  // return cached map if possible
  JvmtiCachedClassFieldMap* cached_map = ik->jvmti_cached_class_field_map();
  if (cached_map != NULL) {
    assert(cached_map->field_map() != NULL, "missing field list");
    return cached_map->field_map();
  } else {
    ClassFieldMap* field_map = ClassFieldMap::create_map_of_instance_fields(obj);
    cached_map = new JvmtiCachedClassFieldMap(field_map);
    ik->set_jvmti_cached_class_field_map(cached_map);
    add_to_class_list(ik);
    return field_map;
  }
}

// remove the fields maps cached from all instanceKlasses
void JvmtiCachedClassFieldMap::clear_cache() {
  assert(Thread::current()->is_VM_thread(), "must be VMThread");
  if (_class_list != NULL) {
    for (int i = 0; i < _class_list->length(); i++) {
      InstanceKlass* ik = _class_list->at(i);
      JvmtiCachedClassFieldMap* cached_map = ik->jvmti_cached_class_field_map();
      assert(cached_map != NULL, "should not be NULL");
      ik->set_jvmti_cached_class_field_map(NULL);
      delete cached_map;  // deletes the encapsulated field map
    }
    delete _class_list;
    _class_list = NULL;
  }
}

// returns the number of ClassFieldMap cached by instanceKlasses
int JvmtiCachedClassFieldMap::cached_field_map_count() {
  return (_class_list == NULL) ? 0 : _class_list->length();
}