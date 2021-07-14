#ifndef SHARE_VM_PRIMS_JVM_HELPTOOLS_HPP
#define SHARE_VM_PRIMS_JVM_HELPTOOLS_HPP


// Helper class used to describe the static or instance fields of a class.
// For each field it holds the field index (as defined by the JVMTI specification),
// the field type, and the offset.

class ClassFieldDescriptor: public CHeapObj<mtInternal> {
 private:
  int _field_index;
  int _field_offset;
  char _field_type;
 public:
  ClassFieldDescriptor(int index, char type, int offset) :
    _field_index(index), _field_type(type), _field_offset(offset) {
  }
  int field_index()  const  { return _field_index; }
  char field_type()  const  { return _field_type; }
  int field_offset() const  { return _field_offset; }
};

class ClassFieldMap: public CHeapObj<mtInternal> {
 private:
  enum {
    initial_field_count = 5
  };

  // list of field descriptors
  GrowableArray<ClassFieldDescriptor*>* _fields;

  // constructor
  ClassFieldMap();

  // add a field
  void add(int index, char type, int offset);

  // returns the field count for the given class
  static int compute_field_count(InstanceKlass* ik);

 public:
  ~ClassFieldMap();

  // access
  int field_count()                     { return _fields->length(); }
  ClassFieldDescriptor* field_at(int i) { return _fields->at(i); }

  // functions to create maps of static or instance fields
  static ClassFieldMap* create_map_of_static_fields(Klass* k);
  static ClassFieldMap* create_map_of_instance_fields(oop obj);
  static ClassFieldMap* create_map_of_instance_obj_fields(oop obj);
};

// Helper class used to cache a ClassFileMap for the instance fields of
// a cache. A JvmtiCachedClassFieldMap can be cached by an InstanceKlass during
// heap iteration and avoid creating a field map for each object in the heap
// (only need to create the map when the first instance of a class is encountered).
//
class JvmtiCachedClassFieldMap : public CHeapObj<mtInternal> {
 private:
   enum {
     initial_class_count = 200
   };
  ClassFieldMap* _field_map;

  ClassFieldMap* field_map() const          { return _field_map; }

  JvmtiCachedClassFieldMap(ClassFieldMap* field_map);
  ~JvmtiCachedClassFieldMap();

  static GrowableArray<InstanceKlass*>* _class_list;
  static void add_to_class_list(InstanceKlass* ik);

 public:
  // returns the field map for a given object (returning map cached
  // by InstanceKlass if possible
  static ClassFieldMap* get_map_of_instance_fields(oop obj);

  // removes the field map from all instanceKlasses - should be
  // called before VM operation completes
  static void clear_cache();

  // returns the number of ClassFieldMap cached by instanceKlasses
  static int cached_field_map_count();
};

// Marker class to ensure that the class file map cache is only used in a defined
// scope.
class ClassFieldMapCacheMark : public StackObj {
 private:
   static bool _is_active;
 public:
   ClassFieldMapCacheMark() {
     assert(Thread::current()->is_VM_thread(), "must be VMThread");
     assert(JvmtiCachedClassFieldMap::cached_field_map_count() == 0, "cache not empty");
     assert(!_is_active, "ClassFieldMapCacheMark cannot be nested");
     _is_active = true;
   }
   ~ClassFieldMapCacheMark() {
     JvmtiCachedClassFieldMap::clear_cache();
     _is_active = false;
   }
   static bool is_active() { return _is_active; }
};

// ObjectMarker is used to support the marking objects when walking the
// heap.
//
// This implementation uses the existing mark bits in an object for
// marking. Objects that are marked must later have their headers restored.
// As most objects are unlocked and don't have their identity hash computed
// we don't have to save their headers. Instead we save the headers that
// are "interesting". Later when the headers are restored this implementation
// restores all headers to their initial value and then restores the few
// objects that had interesting headers.
//
// Future work: This implementation currently uses growable arrays to save
// the oop and header of interesting objects. As an optimization we could
// use the same technique as the GC and make use of the unused area
// between top() and end().
//

// An ObjectClosure used to restore the mark bits of an object
class RestoreMarksClosure : public ObjectClosure {
 public:
  void do_object(oop o) {
    if (o != NULL) {
      markOop mark = o->mark();
      if (mark->is_marked()) {
        o->init_mark();
      }
    }
  }
};


#endif // SHARE_VM_PRIMS_JVM_HELPTOOLS_HPP
