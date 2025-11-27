"""
NiFi Python Processor Template
==============================
SATU FILE - TIDAK ADA IMPORT EXTERNAL MODULE

Cara pakai:
1. Copy file ini, rename sesuai nama processor
2. Cari "# === EDIT ===" dan edit bagian tersebut
3. Bagian "# === JANGAN DIUBAH ===" biarkan saja

Place di: $NIFI_HOME/python/extensions/NamaProcessor.py
"""

import datetime
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.relationship import Relationship

try:
    from nifiapi.properties import PropertyDescriptor as PyPropertyDescriptor, StandardValidators
    _HAS_PY_PD = True
except Exception:
    PyPropertyDescriptor = None
    StandardValidators = None
    _HAS_PY_PD = False


# ==============================================================================
# === JANGAN DIUBAH - Boilerplate Classes & Functions ===
# ==============================================================================

class JVMPropertyDescriptorWrapper:
    def __init__(self, prop_def):
        self.prop_def = prop_def

    def to_java_descriptor(self, gateway, controller_service_type_lookup):
        jvm = gateway.jvm
        try:
            PDBuilder = jvm.org.apache.nifi.components.PropertyDescriptor.Builder
            Validators = jvm.org.apache.nifi.processor.util.StandardValidators
            b = PDBuilder()
            try: b = b.name(self.prop_def["name"])
            except: pass
            try: b = b.displayName(self.prop_def["display"])
            except: pass
            try: b = b.description(self.prop_def["description"])
            except: pass
            try: b = b.defaultValue(self.prop_def["default"])
            except: pass
            try: b = b.required(self.prop_def.get("required", False))
            except: pass
            try:
                v = self.prop_def.get("validator")
                if v == "NON_EMPTY": b = b.addValidator(Validators.NON_EMPTY_VALIDATOR)
                elif v == "BOOLEAN": b = b.addValidator(Validators.BOOLEAN_VALIDATOR)
                elif v == "INTEGER": b = b.addValidator(Validators.INTEGER_VALIDATOR)
                elif v == "POSITIVE_INTEGER": b = b.addValidator(Validators.POSITIVE_INTEGER_VALIDATOR)
            except: pass
            return b.build()
        except Exception as e:
            raise


def _read_prop(context, name, default=""):
    """Baca property value dari context."""
    try: pv = context.getProperty(name)
    except: return default
    if pv is None: return default
    try:
        if hasattr(pv, "getValue"):
            val = pv.getValue()
            return val if val is not None else default
    except: pass
    try: return str(pv) or default
    except: return default


def _parse_bool(val, default=False):
    """Parse string jadi boolean."""
    if val is None: return default
    return str(val).strip().lower() in ("1", "true", "yes", "y")


def _timestamp():
    """Get ISO UTC timestamp."""
    return datetime.datetime.utcnow().isoformat() + "Z"


def _get_property_descriptors(property_defs):
    """Build property descriptors dari PROPERTY_DEFS."""
    if _HAS_PY_PD:
        try:
            pds = set()
            for p in property_defs:
                try: builder = PyPropertyDescriptor.Builder()
                except:
                    try: builder = PyPropertyDescriptor.builder()
                    except: builder = None
                pd = None
                if builder:
                    try:
                        try: builder = builder.name(p["name"])
                        except: pass
                        try: builder = builder.displayName(p["display"])
                        except: pass
                        try: builder = builder.description(p["description"])
                        except: pass
                        try: builder = builder.defaultValue(p["default"])
                        except: pass
                        try: builder = builder.required(p.get("required", False))
                        except: pass
                        try:
                            v = p.get("validator")
                            if v == "NON_EMPTY": builder = builder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                            elif v == "BOOLEAN": builder = builder.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                        except: pass
                        try: pd = builder.build()
                        except: pd = None
                    except: pd = None
                pds.add(pd if pd else JVMPropertyDescriptorWrapper(p))
            return pds
        except: pass
    return {JVMPropertyDescriptorWrapper(p) for p in property_defs}


# ==============================================================================
# === EDIT - Processor Class ===
# ==============================================================================

class MyProcessor(FlowFileTransform):  # <-- GANTI NAMA CLASS
    """
    Deskripsi processor kamu di sini.
    """
    
    # === WAJIB - Jangan diubah formatnya ===
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    # === EDIT - Metadata Processor ===
    class ProcessorDetails:
        version = "1.0.0"
        description = "Deskripsi processor"  # <-- EDIT
        tags = ["tag1", "tag2"]              # <-- EDIT
        dependencies = []                     # <-- Tambah PyPI packages jika perlu, contoh: ['requests']

    # === EDIT - Relationships ===
    REL_SUCCESS = Relationship(name="success", description="Berhasil diproses")
    REL_FAILURE = Relationship(name="failure", description="Gagal diproses")
    # Tambah relationship lain jika perlu:
    # REL_INVALID = Relationship(name="invalid", description="Data tidak valid")

    # === EDIT - Properties ===
    # Format: {"name": "...", "display": "...", "description": "...", "default": "...", "validator": "..."}
    # Validator: "NON_EMPTY", "BOOLEAN", "INTEGER", "POSITIVE_INTEGER", atau None
    PROPERTY_DEFS = [
        {
            "name": "My Property",
            "display": "My Property",
            "description": "Deskripsi property",
            "default": "default value",
            "validator": "NON_EMPTY"
        },
        {
            "name": "Enable Feature",
            "display": "Enable Feature",
            "description": "Aktifkan fitur",
            "default": "false",
            "validator": "BOOLEAN"
        },
        # Tambah property lain...
    ]

    # === JANGAN DIUBAH - Constructor & Boilerplate Methods ===
    def __init__(self, jvm=None, *args, **kwargs):
        self.jvm = jvm
        super().__init__()

    def getRelationships(self):
        # Return semua REL_* yang didefinisikan
        return {self.REL_SUCCESS, self.REL_FAILURE}
        # Jika ada tambahan: return {self.REL_SUCCESS, self.REL_FAILURE, self.REL_INVALID}

    def getPropertyDescriptors(self):
        return _get_property_descriptors(self.PROPERTY_DEFS)

    # === EDIT - onScheduled (Opsional) ===
    def onScheduled(self, context):
        """
        Dipanggil SEKALI saat processor di-start.
        Gunakan untuk inisialisasi, compile regex, buka connection, dll.
        """
        try:
            # Baca properties dan cache
            self.my_property = _read_prop(context, "My Property", "default value")
            self.enable_feature = _parse_bool(_read_prop(context, "Enable Feature", "false"))
            
            # Log
            self.logger.info(f"Scheduled: my_property={self.my_property}, enable_feature={self.enable_feature}")
        except Exception as e:
            try: self.logger.error(f"onScheduled error: {e}")
            except: pass

    # === EDIT - transform (WAJIB) ===
    def transform(self, context, flowfile):
        """
        Dipanggil untuk SETIAP FlowFile.
        
        flowfile methods:
          - flowfile.getContentsAsBytes() -> bytes
          - flowfile.getAttribute("name") -> str
          - flowfile.getAttributes() -> dict
          - flowfile.getSize() -> int
        """
        try:
            # --- Baca FlowFile content ---
            content_bytes = flowfile.getContentsAsBytes()
            content = content_bytes.decode('utf-8') if content_bytes else ""
            
            # --- Baca attributes ---
            attrs = dict(flowfile.getAttributes()) if flowfile.getAttributes() else {}
            
            # --- LOGIC KAMU DI SINI ---
            output = content
            
            if self.enable_feature:
                output = output.upper()
            
            # --- Update attributes ---
            attrs["processed_by"] = "MyProcessor"
            attrs["processed_at"] = _timestamp()
            
            # --- Return success ---
            return FlowFileTransformResult(
                relationship="success",
                contents=output,
                attributes=attrs
            )
            
        except Exception as e:
            try: self.logger.error(f"Transform error: {e}")
            except: pass
            return FlowFileTransformResult(
                relationship="failure",
                contents="",
                attributes={"error": str(e)}
            )