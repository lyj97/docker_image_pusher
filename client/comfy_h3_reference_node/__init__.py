"""Preserve reference video duration on H3's required 24 fps timeline."""
import math


class H3ReferenceFrames:
    @classmethod
    def INPUT_TYPES(cls):
        return {'required': {'images': ('IMAGE',), 'fps': ('FLOAT', {'forceInput': True})}}

    RETURN_TYPES = ('IMAGE',)
    FUNCTION = 'resample'
    CATEGORY = 'H3/reference'

    def resample(self, images, fps):
        if not math.isfinite(float(fps)) or fps <= 0 or len(images) == 0:
            raise ValueError('reference video has invalid frame rate or no frames')
        import torch
        count = max(1, round(len(images) * 24 / fps))
        indices = (torch.arange(count, device=images.device) * fps / 24).long()
        return (images[indices.clamp(max=len(images) - 1)],)


NODE_CLASS_MAPPINGS = {'H3ReferenceFrames': H3ReferenceFrames}
