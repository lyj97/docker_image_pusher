"""Adapt raw AV masks to the fixed stock sampler; no model or sampler replacement."""

class H3AVMaskPrepare:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"latent": ("LATENT",)}}
    RETURN_TYPES = ("LATENT",)
    FUNCTION = "prepare"
    CATEGORY = "H3/compatibility"

    def prepare(self, latent):
        import torch
        import comfy.nested_tensor
        from comfy.sampler_helpers import prepare_mask
        video, audio = latent["samples"].unbind()
        video_mask, audio_mask = latent["noise_mask"].unbind()
        if video.ndim != 5 or audio.ndim != 4 or video_mask.ndim != 3:
            raise ValueError("unexpected H3 AV latent/mask layout")
        if audio_mask.ndim == 2 and audio_mask.shape[1] == 1:
            audio_mask = audio_mask[:, 0]
        if audio_mask.ndim != 1 or any(size <= 0 for size in (*video_mask.shape, *audio_mask.shape)):
            raise ValueError("audio/video masks must have nonempty supported layouts")
        # Preserve the existing stock video's trilinear mask preparation.
        vm = prepare_mask(video_mask, video.shape, video.device)
        am = torch.nn.functional.interpolate(audio_mask.float().reshape(1, 1, -1),
            size=(audio.shape[-1],), mode="nearest-exact")
        am = am.unsqueeze(2).expand(*audio.shape).to(audio.device)
        result = dict(latent)
        result["noise_mask"] = comfy.nested_tensor.NestedTensor((vm, am))
        return (result,)

NODE_CLASS_MAPPINGS = {"H3AVMaskPrepare": H3AVMaskPrepare}
